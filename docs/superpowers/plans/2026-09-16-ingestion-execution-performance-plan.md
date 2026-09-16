# 수집/실행 파이프라인 성능 개선 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 대형 고객사(10곳, 각 100만+ 회원) 규모에서 수집~워크플로 실행 경로가 실제로 버티도록 타임아웃 폴러 테넌트 격리, 참조 데이터 캐시, 멤버 쓰기 증폭 제거, 대기 인스턴스 N+1 배치화, 액션 디스패치 멱등성을 구현한다.

**Architecture:** 포트/어댑터 경계를 유지하면서 (1) `WorkflowInstanceRepositoryPort`/`WorkflowRepositoryPort`에 테넌트 스코프 조회 메서드를 추가하고, (2) Redis 기반 `DistributedLockPort`를 만들어 타임아웃 폴러의 행 단위 락과 액션 디스패치 멱등성 가드 양쪽에 재사용하고, (3) `EventDefinitionPort`/`WorkflowRepositoryPort`를 감싸는 Caffeine 캐시 데코레이터를 `triggerly-bootstrap`에서 `@Primary`로 배선해 기존 유스케이스/컨트롤러 코드는 전혀 변경하지 않고, (4) `IngestEventUseCase`의 멤버 쓰기와 대기 인스턴스 조회를 각각 dirty-tracking과 벌크 IN절로 바꾸고, (5) 타임아웃 폴러를 테넌트별 라운드로빈 + 공유 워커풀 + 테넌트당 세마포어로 재작성한다.

**Tech Stack:** Kotlin 2.3, Spring Boot 4.1, Gradle 멀티모듈(`triggerly-domain`/`triggerly-application`/`triggerly-adapter-in-web`/`triggerly-adapter-out-persistence`/`triggerly-adapter-messaging`/`triggerly-bootstrap`/`triggerly-sdk`), MySQL+Flyway, Redis(Redisson 아님 - Spring Data Redis `StringRedisTemplate`/pub-sub), Kafka, Caffeine(신규), mockk, JUnit5, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-16-ingestion-execution-performance-design.md`

## Global Constraints

- 멀티테넌시 인증/인가는 이 플랜의 스코프가 아니다 (스펙 "스코프 아닌 것" 절).
- Redis 장애 시 정책은 fail-open이다 - 락/캐시/멱등 가드가 Redis에 접근 못 해도 처리 자체를 막지 않는다(스펙 "열린 리스크" 절). `DistributedLockPort`/캐시 어댑터 구현 시 Redis 예외를 흡수해 "락 획득 실패"나 "캐시 미스"로 다루지, 예외를 전파해 메시지 처리 자체를 중단시키지 않는다.
- 타임아웃 폴러 전용 별도 HikariCP 데이터소스는 이 플랜에 포함하지 않는다(스펙의 "후속 과제"로 남겨둠) - 기존 단일 데이터소스를 그대로 쓴다.
- 기존 컨벤션을 따른다: 단위 테스트는 mockk, 통합 테스트는 Testcontainers(`MySqlIntegrationTest`/`RedisIntegrationTest`/`FullStackIntegrationTest`의 "컨테이너를 @Container 없이 직접 start()" 패턴을 그대로 재사용), 포트는 `triggerly-application`, 어댑터는 해당 인프라 모듈, Spring 배선은 `triggerly-bootstrap`.
- 새 Gradle 의존성(Caffeine)은 `gradle/libs.versions.toml`의 버전 카탈로그를 통해 추가한다(직접 문자열 좌표를 build.gradle.kts에 박지 않는다 - 단, `triggerly-application/build.gradle.kts`가 이미 `"org.springframework:spring-context"` 같은 직접 좌표도 섞어 쓰고 있으므로 그 스타일도 허용됨).

---

## Task 1: WorkflowInstanceRepositoryPort에 findAllById / findWaitingExpiredByTenant 추가 + 테넌트 인덱스

**Files:**
- Create: `triggerly-bootstrap/src/main/resources/db/migration/V2__workflow_instance_tenant_waiting_index.sql`
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowInstanceRepositoryPort.kt`
- Modify: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstanceJpaRepository.kt`
- Modify: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapter.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapterTest.kt`
- Modify: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/engine/WorkflowEngineTest.kt` (Fake 구현체가 인터페이스 신규 메서드를 구현해야 컴파일된다)

**Interfaces:**
- Produces: `WorkflowInstanceRepositoryPort.findAllById(ids: Collection<String>): List<WorkflowInstance>` (Task 8이 사용), `WorkflowInstanceRepositoryPort.findWaitingExpiredByTenant(tenantId: String, now: LocalDateTime, limit: Int): List<WorkflowInstance>` (Task 9가 사용). 기존 `findWaitingExpired(now, limit)`는 이번 태스크에서 그대로 유지한다(Task 9에서 옛 폴러와 함께 제거).

- [ ] **Step 1: 마이그레이션 파일 작성**

```sql
CREATE INDEX idx_workflow_instance_tenant_waiting
  ON workflow_instance (tenant_id, status, waiting_until);
```

- [ ] **Step 2: 실패하는 테스트 작성**

`WorkflowInstancePersistenceAdapterTest.kt`에 아래 두 테스트를 추가한다 (파일 전체는 기존 클래스에 이어붙임):

```kotlin
  @Test
  fun `findAllById는 주어진 id들만 조회하고 빈 컬렉션이면 빈 리스트를 반환한다`() {
    val a = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING,
      ),
    )
    val b = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING,
      ),
    )

    val result = adapter.findAllById(listOf(a.id, b.id))

    assertEquals(setOf(a.id, b.id), result.map { it.id }.toSet())
    assertEquals(emptyList(), adapter.findAllById(emptyList()))
  }

  @Test
  fun `findWaitingExpiredByTenant는 해당 테넌트의 만료 WAITING 인스턴스만 waitingUntil 오름차순으로 반환한다`() {
    val tenantId = "t-timeout-${System.nanoTime()}"
    val older = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = tenantId, triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(5),
      ),
    )
    val newer = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = tenantId, triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
      ),
    )
    // 다른 테넌트의 만료 인스턴스는 섞이면 안 된다
    adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "other-tenant", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
      ),
    )

    val result = adapter.findWaitingExpiredByTenant(tenantId, LocalDateTime.now(), limit = 10)

    assertEquals(listOf(older.id, newer.id), result.map { it.id })
  }
```

- [ ] **Step 3: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*WorkflowInstancePersistenceAdapterTest*"`
Expected: FAIL - `Unresolved reference: findAllById` / `findWaitingExpiredByTenant` (아직 포트/어댑터에 메서드가 없음)

- [ ] **Step 4: 포트에 메서드 추가**

`WorkflowInstanceRepositoryPort.kt` 전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.WorkflowInstance
import java.time.LocalDateTime

interface WorkflowInstanceRepositoryPort {
  fun save(instance: WorkflowInstance): WorkflowInstance
  fun findById(id: String): WorkflowInstance?
  fun findWaitingExpired(now: LocalDateTime, limit: Int = 200): List<WorkflowInstance>

  // IngestEventUseCase.handleBatch가 배치 안의 대기 인스턴스 후보들을 개별 findById 대신 IN절 1회로
  // 조회하기 위한 벌크 메서드 (Task 8 참고).
  fun findAllById(ids: Collection<String>): List<WorkflowInstance>

  // TenantAwareTimeoutPoller(Task 9)가 테넌트별로 소량씩만 조회하기 위한 메서드. 전역 findWaitingExpired와
  // 달리 tenant_id를 필터에 포함해 (tenant_id, status, waiting_until) 인덱스를 그대로 타게 한다.
  fun findWaitingExpiredByTenant(tenantId: String, now: LocalDateTime, limit: Int): List<WorkflowInstance>
}
```

- [ ] **Step 5: JPA 리포지토리에 쿼리 메서드 추가**

`WorkflowInstanceJpaRepository.kt` 전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import java.time.LocalDateTime

interface WorkflowInstanceJpaRepository : JpaRepository<WorkflowInstanceEntity, String> {
  fun findAllByStatusAndWaitingUntilLessThanEqual(
    status: String,
    waitingUntil: LocalDateTime,
    pageable: Pageable,
  ): List<WorkflowInstanceEntity>

  fun findAllByTenantIdAndStatusAndWaitingUntilLessThanEqualOrderByWaitingUntilAsc(
    tenantId: String,
    status: String,
    waitingUntil: LocalDateTime,
    pageable: Pageable,
  ): List<WorkflowInstanceEntity>
}
```

- [ ] **Step 6: 어댑터에 구현 추가**

`WorkflowInstancePersistenceAdapter.kt`의 클래스 본문에 아래 두 메서드를 `findWaitingExpired` 구현 바로 아래에 추가:

```kotlin
  override fun findAllById(ids: Collection<String>): List<WorkflowInstance> {
    if (ids.isEmpty()) return emptyList()
    return repository.findAllById(ids).map { it.toDomain() }
  }

  override fun findWaitingExpiredByTenant(tenantId: String, now: LocalDateTime, limit: Int): List<WorkflowInstance> =
    repository.findAllByTenantIdAndStatusAndWaitingUntilLessThanEqualOrderByWaitingUntilAsc(
      tenantId, WorkflowInstanceStatus.WAITING.name, now, PageRequest.of(0, limit),
    ).map { it.toDomain() }
```

- [ ] **Step 7: WorkflowEngineTest의 Fake 구현체 갱신**

`WorkflowEngineTest.kt`의 `FakeWorkflowInstanceRepository`에 아래 두 메서드를 `findWaitingExpired` 구현 바로 아래에 추가:

```kotlin
  override fun findAllById(ids: Collection<String>): List<WorkflowInstance> = ids.mapNotNull { store[it] }
  override fun findWaitingExpiredByTenant(tenantId: String, now: LocalDateTime, limit: Int) =
    store.values.filter { it.tenantId == tenantId && it.status == WorkflowInstanceStatus.WAITING && it.waitingUntil?.isAfter(now) == false }
      .take(limit)
```

- [ ] **Step 8: 전체 컴파일 + 테스트 재실행 (Docker 필요)**

Run: `./gradlew :triggerly-adapter-out-persistence:test :triggerly-application:test --tests "*WorkflowInstancePersistenceAdapterTest*" --tests "*WorkflowEngineTest*"`
Expected: PASS (MySQL/ES Testcontainers가 뜨는 통합 테스트라 Docker 필요 - 로컬에 Docker가 없으면 최소 `./gradlew :triggerly-adapter-out-persistence:compileTestKotlin :triggerly-application:test`로 컴파일과 순수 단위 테스트만이라도 확인한다)

- [ ] **Step 9: 커밋**

```bash
git add triggerly-bootstrap/src/main/resources/db/migration/V2__workflow_instance_tenant_waiting_index.sql \
  triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowInstanceRepositoryPort.kt \
  triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstanceJpaRepository.kt \
  triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapter.kt \
  triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapterTest.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/engine/WorkflowEngineTest.kt
git commit -m "feat(persistence): WorkflowInstanceRepositoryPort에 findAllById/findWaitingExpiredByTenant 추가"
```

---

## Task 2: WorkflowRepositoryPort에 findDistinctTenantIds 추가

**Files:**
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowRepositoryPort.kt`
- Modify: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowJpaRepository.kt`
- Modify: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowPersistenceAdapter.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowPersistenceAdapterTest.kt`

**Interfaces:**
- Consumes: 없음 (독립적인 조회 메서드 추가)
- Produces: `WorkflowRepositoryPort.findDistinctTenantIds(): Set<String>` (Task 4 캐시 데코레이터, Task 9 타임아웃 폴러가 사용)

- [ ] **Step 1: 실패하는 테스트 작성**

`WorkflowPersistenceAdapterTest.kt`에 아래 테스트 추가:

```kotlin
  @Test
  fun `findDistinctTenantIds는 워크플로가 존재하는 테넌트를 중복 없이 반환한다`() {
    val tenantA = "t-distinct-a-${System.nanoTime()}"
    val tenantB = "t-distinct-b-${System.nanoTime()}"
    adapter.save(sampleWorkflow("wf-distinct-1", WorkflowStatus.ENABLED).let {
      Workflow(id = it.id, tenantId = tenantA, triggerEventCode = it.triggerEventCode, definitionJson = it.definitionJson, status = it.status, createdAt = it.createdAt, lastUpdatedAt = it.lastUpdatedAt)
    })
    adapter.save(sampleWorkflow("wf-distinct-2", WorkflowStatus.DRAFT).let {
      Workflow(id = it.id, tenantId = tenantA, triggerEventCode = it.triggerEventCode, definitionJson = it.definitionJson, status = it.status, createdAt = it.createdAt, lastUpdatedAt = it.lastUpdatedAt)
    })
    adapter.save(sampleWorkflow("wf-distinct-3", WorkflowStatus.ENABLED).let {
      Workflow(id = it.id, tenantId = tenantB, triggerEventCode = it.triggerEventCode, definitionJson = it.definitionJson, status = it.status, createdAt = it.createdAt, lastUpdatedAt = it.lastUpdatedAt)
    })

    val result = adapter.findDistinctTenantIds()

    assertTrue(result.containsAll(setOf(tenantA, tenantB)))
  }
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*WorkflowPersistenceAdapterTest*"`
Expected: FAIL - `Unresolved reference: findDistinctTenantIds`

- [ ] **Step 3: 포트에 메서드 추가**

`WorkflowRepositoryPort.kt` 전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Workflow

interface WorkflowRepositoryPort {
  fun save(workflow: Workflow): Workflow
  fun findById(tenantId: String, id: String): Workflow?
  fun findAll(tenantId: String): List<Workflow>
  fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow>

  // TenantAwareTimeoutPoller(Task 9)가 "이번 틱에 순회할 테넌트 목록"의 소스로 쓴다 - workflow_instance
  // 대신 훨씬 작고 안정적인 workflow 테이블을 스캔한다.
  fun findDistinctTenantIds(): Set<String>
}
```

- [ ] **Step 4: JPA 리포지토리에 쿼리 메서드 추가**

`WorkflowJpaRepository.kt` 전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

interface WorkflowJpaRepository : JpaRepository<WorkflowEntity, String> {
  fun findByIdAndTenantId(id: String, tenantId: String): WorkflowEntity?
  fun findAllByTenantId(tenantId: String): List<WorkflowEntity>
  fun findAllByTenantIdAndTriggerEventCodeAndStatus(tenantId: String, triggerEventCode: String, status: String): List<WorkflowEntity>

  @Query("SELECT DISTINCT w.tenantId FROM WorkflowEntity w")
  fun findDistinctTenantIds(): Set<String>
}
```

- [ ] **Step 5: 어댑터에 구현 추가**

`WorkflowPersistenceAdapter.kt`의 `findEnabledByTriggerEventCode` 구현 바로 아래에 추가:

```kotlin
  override fun findDistinctTenantIds(): Set<String> = repository.findDistinctTenantIds()
```

- [ ] **Step 6: 테스트 재실행 (Docker 필요)**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*WorkflowPersistenceAdapterTest*"`
Expected: PASS

- [ ] **Step 7: 커밋**

```bash
git add triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowRepositoryPort.kt \
  triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowJpaRepository.kt \
  triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowPersistenceAdapter.kt \
  triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowPersistenceAdapterTest.kt
git commit -m "feat(persistence): WorkflowRepositoryPort에 findDistinctTenantIds 추가"
```

---

## Task 3: DistributedLockPort + RedisDistributedLockAdapter

**Files:**
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/DistributedLockPort.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisDistributedLockAdapter.kt`
- Test: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisDistributedLockAdapterTest.kt`

**Interfaces:**
- Produces: `DistributedLockPort.tryLock(key: String, ttl: Duration): Boolean` (Task 9 타임아웃 폴러의 행 단위 락, Task 10 액션 디스패치 멱등성 가드 양쪽이 재사용)

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RedisDistributedLockAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisDistributedLockAdapter

  @Test
  fun `같은 키로 두 번 tryLock하면 두 번째는 실패한다`() {
    val key = "lock:test:${System.nanoTime()}"
    assertTrue(adapter.tryLock(key, Duration.ofSeconds(10)))
    assertFalse(adapter.tryLock(key, Duration.ofSeconds(10)))
  }

  @Test
  fun `ttl이 지나면 같은 키를 다시 tryLock할 수 있다`() {
    val key = "lock:ttl:${System.nanoTime()}"
    assertTrue(adapter.tryLock(key, Duration.ofMillis(200)))
    Thread.sleep(300)
    assertTrue(adapter.tryLock(key, Duration.ofMillis(200)))
  }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisDistributedLockAdapterTest*"`
Expected: FAIL - `Unresolved reference: RedisDistributedLockAdapter`

- [ ] **Step 3: 포트 작성**

```kotlin
package com.seaotter.triggerly.application.port

import java.time.Duration

// SET NX PX 기반 분산 락. tryLock이 true를 반환하면 호출자가 그 키의 유일한 소유자다 - ttl이 지나면
// 자동 해제되므로 명시적 unlock이 없다. 짧은 TTL로는 타임아웃 폴러의 행 단위 락(여러 인스턴스가 같은
// 만료 인스턴스를 동시에 집었을 때 한쪽만 처리)으로, 긴 TTL로는 액션 디스패치 멱등성 가드("이 dispatchId는
// 이미 처리했다"는 표시)로 재사용한다 - 두 용도 모두 "한 번 선점하면 그걸로 끝"이라 unlock이 필요 없다.
fun interface DistributedLockPort {
  fun tryLock(key: String, ttl: Duration): Boolean
}
```

- [ ] **Step 4: 어댑터 작성**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.DistributedLockPort
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RedisDistributedLockAdapter(
  private val redisTemplate: StringRedisTemplate,
) : DistributedLockPort {

  override fun tryLock(key: String, ttl: Duration): Boolean =
    redisTemplate.opsForValue().setIfAbsent(key, "1", ttl) ?: false
}
```

- [ ] **Step 5: 테스트 재실행 (Docker 필요)**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisDistributedLockAdapterTest*"`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/DistributedLockPort.kt \
  triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisDistributedLockAdapter.kt \
  triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisDistributedLockAdapterTest.kt
git commit -m "feat(messaging): Redis 기반 DistributedLockPort 추가"
```

---

## Task 4: 참조 데이터 캐시 데코레이터 (Caffeine, pub/sub 없이 TTL만)

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `triggerly-application/build.gradle.kts`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/cache/CachedEventDefinitionAdapter.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/cache/CachedWorkflowRepositoryAdapter.kt`
- Test: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/cache/CachedEventDefinitionAdapterTest.kt`
- Test: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/cache/CachedWorkflowRepositoryAdapterTest.kt`

**Interfaces:**
- Consumes: `EventDefinitionPort`, `WorkflowRepositoryPort` (기존), `WorkflowRepositoryPort.findDistinctTenantIds()` (Task 2)
- Produces: `CachedEventDefinitionAdapter(delegate: EventDefinitionPort, ttl: Duration = 60s, ticker: Ticker = systemTicker())` (public `fun evict(tenantId, code)`), `CachedWorkflowRepositoryAdapter(delegate: WorkflowRepositoryPort, ttl: Duration = 60s, ticker: Ticker = systemTicker())` (public `fun evict(tenantId, workflowId)`) - 둘 다 `triggerly-application`의 평범한 클래스(스프링 어노테이션 없음), Task 6에서 bootstrap이 `@Bean`으로 감싸 배선한다.

- [ ] **Step 1: 버전 카탈로그에 Caffeine 추가**

`gradle/libs.versions.toml`의 `[versions]` 블록에 추가:

```toml
caffeine = "3.2.0"
```

`[libraries]` 블록에 추가:

```toml
caffeine = { module = "com.github.ben-manes.caffeine:caffeine", version.ref = "caffeine" }
```

- [ ] **Step 2: triggerly-application에 의존성 추가**

`triggerly-application/build.gradle.kts`의 `dependencies` 블록에 추가:

```kotlin
	implementation(libs.caffeine)
```

- [ ] **Step 3: 실패하는 테스트 작성 (EventDefinition 캐시)**

```kotlin
package com.seaotter.triggerly.application.cache

import com.github.benmanes.caffeine.cache.Ticker
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CachedEventDefinitionAdapterTest {

  private val delegate = mockk<EventDefinitionPort>()

  @Test
  fun `동일 tenantId+code를 반복 조회해도 delegate는 한 번만 호출된다`() {
    val definition = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.findByTenantAndCode("t1", "PURCHASE") } returns definition
    val cache = CachedEventDefinitionAdapter(delegate)

    repeat(3) { assertEquals(definition, cache.findByTenantAndCode("t1", "PURCHASE")) }

    verify(exactly = 1) { delegate.findByTenantAndCode("t1", "PURCHASE") }
  }

  @Test
  fun `evict 이후에는 delegate를 다시 호출한다`() {
    val definition = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.findByTenantAndCode("t1", "PURCHASE") } returns definition
    val cache = CachedEventDefinitionAdapter(delegate)

    cache.findByTenantAndCode("t1", "PURCHASE")
    cache.evict("t1", "PURCHASE")
    cache.findByTenantAndCode("t1", "PURCHASE")

    verify(exactly = 2) { delegate.findByTenantAndCode("t1", "PURCHASE") }
  }

  @Test
  fun `TTL이 지나면 delegate를 다시 호출한다`() {
    val definition = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.findByTenantAndCode("t1", "PURCHASE") } returns definition
    val nanos = AtomicLong(0)
    val ticker = Ticker { nanos.get() }
    val cache = CachedEventDefinitionAdapter(delegate, ttl = Duration.ofSeconds(60), ticker = ticker)

    cache.findByTenantAndCode("t1", "PURCHASE")
    nanos.set(Duration.ofSeconds(61).toNanos())
    cache.findByTenantAndCode("t1", "PURCHASE")

    verify(exactly = 2) { delegate.findByTenantAndCode("t1", "PURCHASE") }
  }

  @Test
  fun `존재하지 않는 정의도 null로 캐시되어 delegate를 반복 호출하지 않는다`() {
    every { delegate.findByTenantAndCode("t1", "UNKNOWN") } returns null
    val cache = CachedEventDefinitionAdapter(delegate)

    assertNull(cache.findByTenantAndCode("t1", "UNKNOWN"))
    assertNull(cache.findByTenantAndCode("t1", "UNKNOWN"))

    verify(exactly = 1) { delegate.findByTenantAndCode("t1", "UNKNOWN") }
  }

  @Test
  fun `findAll과 save는 캐시를 거치지 않고 delegate에 그대로 위임한다`() {
    every { delegate.findAll("t1") } returns emptyList()
    val saved = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.save(any()) } returns saved
    val cache = CachedEventDefinitionAdapter(delegate)

    cache.findAll("t1")
    cache.save(saved)

    verify(exactly = 1) { delegate.findAll("t1") }
    verify(exactly = 1) { delegate.save(saved) }
  }
}
```

- [ ] **Step 4: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*CachedEventDefinitionAdapterTest*"`
Expected: FAIL - `Unresolved reference: CachedEventDefinitionAdapter`

- [ ] **Step 5: CachedEventDefinitionAdapter 구현**

```kotlin
package com.seaotter.triggerly.application.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import java.time.Duration
import java.util.Optional

// EventIngestionController가 이벤트 하나를 받을 때마다 EventDefinitionPort.findByTenantAndCode를
// 캐시 없이 호출하던 것을 없애기 위한 read-through 캐시. save/findAll(관리자 CRUD 경로, 호출 빈도가
// 낮음)은 캐시하지 않고 delegate에 그대로 위임한다. EventDefinition.displayName은 var라 이론적으로는
// 가변이지만, 이 코드베이스에서 EventDefinitionPort를 읽었다가 그대로 mutate해서 되돌려 쓰는 호출부가
// 없어(ManageEventDefinitionUseCase.register는 매번 새 인스턴스를 만들어 저장) 방어적 복사가 필요 없다.
// evict()는 Redis pub/sub 무효화 메시지를 받았을 때 호출된다(ReferenceDataCacheConfig, Task 6 참고) -
// 이 클래스 자신은 Redis를 전혀 모른다.
class CachedEventDefinitionAdapter(
  private val delegate: EventDefinitionPort,
  ttl: Duration = Duration.ofSeconds(60),
  ticker: Ticker = Ticker.systemTicker(),
) : EventDefinitionPort {

  private val cache = Caffeine.newBuilder()
    .expireAfterWrite(ttl)
    .ticker(ticker)
    .build<String, Optional<EventDefinition>>()

  override fun save(definition: EventDefinition): EventDefinition = delegate.save(definition)

  override fun findByTenantAndCode(tenantId: String, code: String): EventDefinition? =
    cache.get(cacheKey(tenantId, code)) { Optional.ofNullable(delegate.findByTenantAndCode(tenantId, code)) }
      .orElse(null)

  override fun findAll(tenantId: String): List<EventDefinition> = delegate.findAll(tenantId)

  fun evict(tenantId: String, code: String) {
    cache.invalidate(cacheKey(tenantId, code))
  }

  private fun cacheKey(tenantId: String, code: String) = "$tenantId:$code"
}
```

- [ ] **Step 6: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*CachedEventDefinitionAdapterTest*"`
Expected: PASS

- [ ] **Step 7: 실패하는 테스트 작성 (Workflow 캐시 - 방어적 복사 검증 포함)**

```kotlin
package com.seaotter.triggerly.application.cache

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Edge
import com.seaotter.triggerly.domain.EdgeRoute
import com.seaotter.triggerly.domain.Node
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame

class CachedWorkflowRepositoryAdapterTest {

  private val delegate = mockk<WorkflowRepositoryPort>()

  private fun sampleWorkflow() = Workflow(
    id = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
    definitionJson = WorkflowDefinition("LOGIN", listOf(Node.Trigger("n1", "LOGIN"), Node.End("n2")), listOf(Edge("n1", "n2", EdgeRoute.Always))),
    status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )

  @Test
  fun `findById를 반복 호출해도 delegate는 한 번만 호출된다`() {
    every { delegate.findById("t1", "wf-1") } returns sampleWorkflow()
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    repeat(3) { cache.findById("t1", "wf-1") }

    verify(exactly = 1) { delegate.findById("t1", "wf-1") }
  }

  @Test
  fun `findById가 반환한 객체를 호출자가 mutate해도 캐시된 값에는 영향이 없다`() {
    every { delegate.findById("t1", "wf-1") } returns sampleWorkflow()
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    val first = cache.findById("t1", "wf-1")!!
    assertEquals(WorkflowStatus.DRAFT, first.status)
    first.status = WorkflowStatus.ENABLED // ManageWorkflowUseCase.enable()과 동일한 read-modify-write

    val second = cache.findById("t1", "wf-1")!!
    assertNotSame(first, second)
    assertEquals(WorkflowStatus.DRAFT, second.status)
  }

  @Test
  fun `findEnabledByTriggerEventCode를 반복 호출해도 delegate는 한 번만 호출된다`() {
    every { delegate.findEnabledByTriggerEventCode("t1", "LOGIN") } returns listOf(sampleWorkflow())
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    repeat(3) { cache.findEnabledByTriggerEventCode("t1", "LOGIN") }

    verify(exactly = 1) { delegate.findEnabledByTriggerEventCode("t1", "LOGIN") }
  }

  @Test
  fun `findDistinctTenantIds를 반복 호출해도 delegate는 한 번만 호출된다`() {
    every { delegate.findDistinctTenantIds() } returns setOf("t1", "t2")
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    repeat(3) { cache.findDistinctTenantIds() }

    verify(exactly = 1) { delegate.findDistinctTenantIds() }
  }

  @Test
  fun `evict 이후에는 findById가 delegate를 다시 호출한다`() {
    every { delegate.findById("t1", "wf-1") } returns sampleWorkflow()
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    cache.findById("t1", "wf-1")
    cache.evict("t1", "wf-1")
    cache.findById("t1", "wf-1")

    verify(exactly = 2) { delegate.findById("t1", "wf-1") }
  }

  @Test
  fun `save와 findAll은 캐시를 거치지 않고 delegate에 그대로 위임한다`() {
    every { delegate.findAll("t1") } returns emptyList()
    val saved = sampleWorkflow()
    every { delegate.save(any()) } returns saved
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    cache.findAll("t1")
    cache.save(saved)

    verify(exactly = 1) { delegate.findAll("t1") }
    verify(exactly = 1) { delegate.save(saved) }
  }
}
```

- [ ] **Step 8: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*CachedWorkflowRepositoryAdapterTest*"`
Expected: FAIL - `Unresolved reference: CachedWorkflowRepositoryAdapter`

- [ ] **Step 9: CachedWorkflowRepositoryAdapter 구현**

```kotlin
package com.seaotter.triggerly.application.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import java.time.Duration
import java.util.Optional

// EventIngestionController/IngestEventUseCase/TenantAwareTimeoutPoller가 워크플로 정의·테넌트 목록을
// 반복 조회하는 것을 막기 위한 read-through 캐시. save/findAll(관리자 CRUD, 저빈도)은 캐시하지 않는다.
class CachedWorkflowRepositoryAdapter(
  private val delegate: WorkflowRepositoryPort,
  ttl: Duration = Duration.ofSeconds(60),
  ticker: Ticker = Ticker.systemTicker(),
) : WorkflowRepositoryPort {

  private val byIdCache = Caffeine.newBuilder().expireAfterWrite(ttl).ticker(ticker)
    .build<String, Optional<Workflow>>()
  private val byTriggerCache = Caffeine.newBuilder().expireAfterWrite(ttl).ticker(ticker)
    .build<String, List<Workflow>>()
  private val tenantIdsCache = Caffeine.newBuilder().expireAfterWrite(ttl).ticker(ticker)
    .build<String, Set<String>>()

  override fun save(workflow: Workflow): Workflow = delegate.save(workflow)

  override fun findAll(tenantId: String): List<Workflow> = delegate.findAll(tenantId)

  // Workflow는 status/lastUpdatedAt이 var인 가변 클래스다. ManageWorkflowUseCase.enable()이 findById로
  // 받은 인스턴스를 그대로 mutate한 뒤 save()에 넘기므로(read-modify-write), 캐시에 저장된 객체를 그대로
  // 반환하면 save 전에 이미 캐시가 오염된다. 그래서 캐시 히트/미스와 무관하게 항상 새 인스턴스를 복사해
  // 반환한다 - 호출자가 뭘 하든 캐시 내부 상태는 절대 바뀌지 않는다.
  override fun findById(tenantId: String, id: String): Workflow? =
    byIdCache.get(idKey(tenantId, id)) { Optional.ofNullable(delegate.findById(tenantId, id)) }
      .orElse(null)
      ?.let(::copyOf)

  // 현재 호출부(IngestEventUseCase)는 반환된 Workflow를 읽기만 하고 mutate하지 않으므로 방어적 복사가
  // 당장은 필요 없다 - 나중에 mutate하는 호출부가 생기면 findById와 동일하게 copyOf를 적용해야 한다.
  override fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow> =
    byTriggerCache.get(triggerKey(tenantId, eventCode)) { delegate.findEnabledByTriggerEventCode(tenantId, eventCode) }

  override fun findDistinctTenantIds(): Set<String> = tenantIdsCache.get(TENANT_IDS_KEY) { delegate.findDistinctTenantIds() }

  fun evict(tenantId: String, workflowId: String) {
    byIdCache.invalidate(idKey(tenantId, workflowId))
    // 워크플로 하나가 바뀌면 그게 어떤 triggerEventCode에 걸려 있었는지 이 클래스는 알 수 없으므로(evict
    // 호출 시점엔 tenantId+workflowId만 전달됨) 안전하게 그 테넌트의 트리거 캐시 전체와 테넌트 목록
    // 캐시를 함께 비운다 - 워크플로 변경은 admin 경로에서만 드물게 발생해 과잉 무효화 비용이 작다.
    byTriggerCache.asMap().keys.removeIf { it.startsWith("$tenantId:") }
    tenantIdsCache.invalidateAll()
  }

  private fun copyOf(workflow: Workflow) = Workflow(
    id = workflow.id, tenantId = workflow.tenantId, name = workflow.name, triggerEventCode = workflow.triggerEventCode,
    definitionJson = workflow.definitionJson, version = workflow.version, status = workflow.status,
    createdAt = workflow.createdAt, lastUpdatedAt = workflow.lastUpdatedAt,
  )

  private fun idKey(tenantId: String, id: String) = "$tenantId:$id"
  private fun triggerKey(tenantId: String, eventCode: String) = "$tenantId:$eventCode"

  companion object {
    private const val TENANT_IDS_KEY = "__tenants__"
  }
}
```

- [ ] **Step 10: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*CachedWorkflowRepositoryAdapterTest*"`
Expected: PASS

- [ ] **Step 11: 커밋**

```bash
git add gradle/libs.versions.toml triggerly-application/build.gradle.kts \
  triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/cache/CachedEventDefinitionAdapter.kt \
  triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/cache/CachedWorkflowRepositoryAdapter.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/cache/CachedEventDefinitionAdapterTest.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/cache/CachedWorkflowRepositoryAdapterTest.kt
git commit -m "feat(application): EventDefinition/Workflow 참조 데이터 Caffeine 캐시 데코레이터 추가"
```

---

## Task 5: 캐시 무효화 pub/sub (CacheInvalidationPort + Redis 어댑터 + 발행 지점)

**Files:**
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/CacheInvalidationPort.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisCacheInvalidationAdapter.kt`
- Test: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisCacheInvalidationAdapterTest.kt`
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageEventDefinitionUseCase.kt`
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageWorkflowUseCase.kt`
- Test: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/ManageEventDefinitionUseCaseTest.kt` (신규)
- Modify: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/ManageWorkflowUseCaseTest.kt`

**Interfaces:**
- Consumes: 없음
- Produces: `CacheInvalidationPort.publish(topic: CacheInvalidationTopic, key: String)`, `CacheInvalidationTopic { EVENT_DEFINITION, WORKFLOW }`, Redis 채널 상수 `EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL = "cache:invalidate:event-definition"`, `WORKFLOW_CACHE_INVALIDATION_CHANNEL = "cache:invalidate:workflow"` (Task 6이 구독)

- [ ] **Step 1: 실패하는 테스트 작성 (pub/sub 왕복)**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RedisCacheInvalidationAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisCacheInvalidationAdapter
  @Autowired lateinit var connectionFactory: RedisConnectionFactory

  @Test
  fun `publish하면 해당 채널을 구독 중인 리스너가 메시지를 받는다`() {
    val latch = CountDownLatch(1)
    var received: String? = null
    val container = RedisMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.addMessageListener(
      MessageListener { message, _ -> received = String(message.body); latch.countDown() },
      ChannelTopic(EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL),
    )
    container.afterPropertiesSet()
    container.start()

    try {
      Thread.sleep(200) // Redis pub/sub는 구독이 실제로 붙은 이후의 메시지만 받으므로 짧게 대기
      adapter.publish(CacheInvalidationTopic.EVENT_DEFINITION, "t1:PURCHASE")
      assertTrue(latch.await(3, TimeUnit.SECONDS))
      assertEquals("t1:PURCHASE", received)
    } finally {
      container.stop()
    }
  }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisCacheInvalidationAdapterTest*"`
Expected: FAIL - `Unresolved reference: RedisCacheInvalidationAdapter`

- [ ] **Step 3: CacheInvalidationPort 작성**

```kotlin
package com.seaotter.triggerly.application.port

enum class CacheInvalidationTopic { EVENT_DEFINITION, WORKFLOW }

// 이 인스턴스가 참조 데이터를 저장한 뒤, 다른 모든 앱 인스턴스(자기 자신 포함)의 캐시를 비우라고
// 알리기 위한 포트. key는 캐시 데코레이터가 evict()에 그대로 넘길 수 있는 형식("tenantId:code" 등)이다.
fun interface CacheInvalidationPort {
  fun publish(topic: CacheInvalidationTopic, key: String)
}
```

- [ ] **Step 4: RedisCacheInvalidationAdapter 작성**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

const val EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL = "cache:invalidate:event-definition"
const val WORKFLOW_CACHE_INVALIDATION_CHANNEL = "cache:invalidate:workflow"

@Component
class RedisCacheInvalidationAdapter(
  private val redisTemplate: StringRedisTemplate,
) : CacheInvalidationPort {

  override fun publish(topic: CacheInvalidationTopic, key: String) {
    val channel = when (topic) {
      CacheInvalidationTopic.EVENT_DEFINITION -> EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL
      CacheInvalidationTopic.WORKFLOW -> WORKFLOW_CACHE_INVALIDATION_CHANNEL
    }
    redisTemplate.convertAndSend(channel, key)
  }
}
```

- [ ] **Step 5: 테스트 재실행 (Docker 필요)**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisCacheInvalidationAdapterTest*"`
Expected: PASS

- [ ] **Step 6: ManageEventDefinitionUseCase에 발행 지점 추가 - 실패하는 테스트 먼저**

`triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/ManageEventDefinitionUseCaseTest.kt` 신규 작성:

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class ManageEventDefinitionUseCaseTest {

  private val port = mockk<EventDefinitionPort>()
  private val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
  private val useCase = ManageEventDefinitionUseCase(port, cacheInvalidationPort)

  @Test
  fun `register는 저장 후 EVENT_DEFINITION 캐시 무효화를 발행한다`() {
    every { port.save(any()) } answers { firstArg() }

    useCase.register("t1", "PURCHASE", "구매")

    verify(exactly = 1) { cacheInvalidationPort.publish(CacheInvalidationTopic.EVENT_DEFINITION, "t1:PURCHASE") }
  }
}
```

- [ ] **Step 7: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*ManageEventDefinitionUseCaseTest*"`
Expected: FAIL - 생성자 인자 개수 불일치 컴파일 에러

- [ ] **Step 8: ManageEventDefinitionUseCase 수정**

전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.stereotype.Service

@Service
class ManageEventDefinitionUseCase(
  private val port: EventDefinitionPort,
  private val cacheInvalidationPort: CacheInvalidationPort,
) {
  fun register(tenantId: String, code: String, displayName: String): EventDefinition {
    val saved = port.save(EventDefinition(tenantId = tenantId, code = code, displayName = displayName))
    cacheInvalidationPort.publish(CacheInvalidationTopic.EVENT_DEFINITION, "$tenantId:$code")
    return saved
  }

  fun list(tenantId: String): List<EventDefinition> = port.findAll(tenantId)
}
```

- [ ] **Step 9: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*ManageEventDefinitionUseCaseTest*"`
Expected: PASS

- [ ] **Step 10: ManageWorkflowUseCase에 발행 지점 추가 - 기존 테스트 갱신**

`ManageWorkflowUseCaseTest.kt`의 각 테스트에서 `val useCase = ManageWorkflowUseCase(port)`를 `val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)` 선언 후 `val useCase = ManageWorkflowUseCase(port, cacheInvalidationPort)`로 바꾸고, 파일 상단에 `import com.seaotter.triggerly.application.port.CacheInvalidationPort`와 `import com.seaotter.triggerly.application.port.CacheInvalidationTopic`를 추가한다. 그리고 아래 테스트를 추가한다:

```kotlin
  @Test
  fun `enable은 성공 후 WORKFLOW 캐시 무효화를 발행한다`() {
    val port = mockk<WorkflowRepositoryPort>()
    val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
    val workflow = Workflow(
      id = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
      definitionJson = WorkflowDefinition("LOGIN", emptyList(), emptyList()),
      status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )
    every { port.findById("t1", "wf-1") } returns workflow
    every { port.save(any()) } answers { firstArg() }

    ManageWorkflowUseCase(port, cacheInvalidationPort).enable("t1", "wf-1")

    verify(exactly = 1) { cacheInvalidationPort.publish(CacheInvalidationTopic.WORKFLOW, "t1:wf-1") }
  }
```

- [ ] **Step 11: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*ManageWorkflowUseCaseTest*"`
Expected: FAIL - 생성자 인자 개수 불일치

- [ ] **Step 12: ManageWorkflowUseCase 수정**

전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Node
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ManageWorkflowUseCase(
  private val port: WorkflowRepositoryPort,
  private val cacheInvalidationPort: CacheInvalidationPort,
) {
  fun create(workflow: Workflow): Workflow {
    validate(workflow.definitionJson)
    val saved = port.save(workflow)
    cacheInvalidationPort.publish(CacheInvalidationTopic.WORKFLOW, "${saved.tenantId}:${saved.id}")
    return saved
  }
  fun list(tenantId: String): List<Workflow> = port.findAll(tenantId)
  fun get(tenantId: String, id: String): Workflow? = port.findById(tenantId, id)

  fun enable(tenantId: String, id: String): Workflow {
    val workflow = port.findById(tenantId, id) ?: error("workflow not found: $id")
    workflow.status = WorkflowStatus.ENABLED
    workflow.lastUpdatedAt = LocalDateTime.now()
    val saved = port.save(workflow)
    cacheInvalidationPort.publish(CacheInvalidationTopic.WORKFLOW, "$tenantId:$id")
    return saved
  }

  private fun validate(definition: WorkflowDefinition) {
    val nodeIds = definition.nodes.map { it.id }.toSet()
    definition.edges.forEach { edge ->
      require(edge.from in nodeIds) { "워크플로 정의에 존재하지 않는 노드를 가리키는 엣지입니다 (from=${edge.from})" }
      require(edge.to in nodeIds) { "워크플로 정의에 존재하지 않는 노드를 가리키는 엣지입니다 (to=${edge.to})" }
    }

    val triggerNodes = definition.nodes.filterIsInstance<Node.Trigger>()
    require(triggerNodes.size == 1) {
      "워크플로 정의에는 정확히 하나의 Trigger 노드가 있어야 합니다 (실제 개수: ${triggerNodes.size})"
    }

    val adjacency = definition.edges.groupBy({ it.from }, { it.to })
    val visited = mutableSetOf<String>()
    val onStack = mutableSetOf<String>()

    fun detectCycle(nodeId: String) {
      if (nodeId in onStack) {
        throw IllegalArgumentException("워크플로 정의에 순환(cycle)이 존재합니다 (노드: $nodeId)")
      }
      if (nodeId in visited) return
      onStack += nodeId
      adjacency[nodeId]?.forEach { detectCycle(it) }
      onStack -= nodeId
      visited += nodeId
    }

    detectCycle(triggerNodes.first().id)
  }
}
```

- [ ] **Step 13: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*ManageWorkflowUseCaseTest*" --tests "*ManageEventDefinitionUseCaseTest*"`
Expected: PASS

- [ ] **Step 14: 커밋**

```bash
git add triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/CacheInvalidationPort.kt \
  triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisCacheInvalidationAdapter.kt \
  triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisCacheInvalidationAdapterTest.kt \
  triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageEventDefinitionUseCase.kt \
  triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageWorkflowUseCase.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/ManageEventDefinitionUseCaseTest.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/ManageWorkflowUseCaseTest.kt
git commit -m "feat(cache): 참조 데이터 저장 시 Redis pub/sub로 캐시 무효화 발행"
```

---

## Task 6: Bootstrap 배선 - 캐시 데코레이터 @Primary 등록 + Redis 구독 + 교차 인스턴스 검증

**Files:**
- Modify: `triggerly-bootstrap/build.gradle.kts`
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/cache/ReferenceDataCacheConfig.kt`
- Test: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/cache/ReferenceDataCacheCrossInstanceTest.kt`

**Interfaces:**
- Consumes: `CachedEventDefinitionAdapter`/`CachedWorkflowRepositoryAdapter`(Task 4), `EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL`/`WORKFLOW_CACHE_INVALIDATION_CHANNEL`(Task 5), `EventDefinitionPersistenceAdapter`/`WorkflowPersistenceAdapter`(기존 어댑터)
- Produces: 없음 (배선 완료가 산출물) - 이후 `EventIngestionController`/`IngestEventUseCase`/`TenantAwareTimeoutPoller`는 코드 변경 없이 캐시된 구현체를 자동으로 주입받는다.

- [ ] **Step 1: bootstrap에 Redis 의존성 명시적으로 추가**

`triggerly-bootstrap/build.gradle.kts`의 `dependencies` 블록에 추가(기존 JPA/ES 스타터를 명시적으로 추가한 것과 같은 이유 - `RedisConnectionFactory`/`RedisMessageListenerContainer`/`ChannelTopic` 타입이 이 모듈 컴파일 클래스패스에 보여야 함):

```kotlin
	implementation(libs.spring.boot.starter.data.redis)
```

- [ ] **Step 2: 실패하는 테스트 작성 (교차 인스턴스 무효화)**

```kotlin
package com.seaotter.triggerly.bootstrap.cache

import com.seaotter.triggerly.adapter.messaging.redis.EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL
import com.seaotter.triggerly.adapter.persistence.jpa.EventDefinitionPersistenceAdapter
import com.seaotter.triggerly.application.cache.CachedEventDefinitionAdapter
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.bootstrap.FullStackIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// "다른 앱 인스턴스"를 흉내내기 위해, 스프링이 관리하는 캐시(이 테스트 프로세스 자체)와는 별개로 같은
// EventDefinitionPersistenceAdapter delegate를 감싼 두 번째 CachedEventDefinitionAdapter(instance B)를
// 수동으로 만들고, 같은 Redis 채널을 구독하는 별도의 RedisMessageListenerContainer를 붙인다.
// ManageEventDefinitionUseCase(instance A 경유)에서 저장이 일어나면 instance B도 Redis pub/sub만으로
// 즉시 evict되는지 검증한다 - 두 인스턴스는 서로를 직접 참조하지 않는다.
class ReferenceDataCacheCrossInstanceTest : FullStackIntegrationTest() {

  @Autowired lateinit var manageEventDefinitionUseCase: ManageEventDefinitionUseCase
  @Autowired lateinit var delegate: EventDefinitionPersistenceAdapter
  @Autowired lateinit var connectionFactory: RedisConnectionFactory

  @Test
  fun `한 인스턴스에서 저장하면 다른 인스턴스의 캐시도 Redis pub-sub만으로 무효화된다`() {
    val tenantId = "t-cross-${System.nanoTime()}"
    manageEventDefinitionUseCase.register(tenantId, "PURCHASE", "구매")

    val instanceBCache = CachedEventDefinitionAdapter(delegate)
    val warmed = instanceBCache.findByTenantAndCode(tenantId, "PURCHASE")
    assertEquals("구매", warmed?.displayName)

    val latch = CountDownLatch(1)
    val container = RedisMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.addMessageListener(
      MessageListener { message, _ ->
        val (t, code) = String(message.body).split(":", limit = 2)
        instanceBCache.evict(t, code)
        latch.countDown()
      },
      ChannelTopic(EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL),
    )
    container.afterPropertiesSet()
    container.start()

    try {
      Thread.sleep(200)
      manageEventDefinitionUseCase.register(tenantId, "PURCHASE", "구매(수정)")

      assertTrue(latch.await(3, TimeUnit.SECONDS), "무효화 메시지를 3초 안에 받지 못했다")
      assertEquals("구매(수정)", instanceBCache.findByTenantAndCode(tenantId, "PURCHASE")?.displayName)
    } finally {
      container.stop()
    }
  }
}
```

- [ ] **Step 3: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-bootstrap:test --tests "*ReferenceDataCacheCrossInstanceTest*"`
Expected: FAIL - `Unresolved reference` (아직 `ReferenceDataCacheConfig`가 없어 `CachedEventDefinitionAdapter`가 `@Primary` 빈으로 등록되지 않았어도 컴파일 자체는 되지만, `manageEventDefinitionUseCase` 빈 생성 시 `CacheInvalidationPort` 빈은 이미 Task 5에서 등록됐으므로 컨텍스트 로딩 자체는 될 수 있음 - 이 단계의 진짜 실패 지점은 다음 단계에서 `ReferenceDataCacheConfig`를 만들기 전까지 `instanceBCache.evict` 관련 import가 없어도 되므로, 실제로는 테스트 자체는 컴파일되지만 **아직 캐시 구독 배선이 없어 latch.await가 타임아웃으로 실패**한다)
Expected(정정): FAIL - `무효화 메시지를 3초 안에 받지 못했다` (instance A 쪽에 캐시 데코레이터 자체가 아직 배선되지 않아, `ManageEventDefinitionUseCase`가 정의를 캐시 없이 그냥 저장만 하고 Redis에 발행은 하지만, 이 발행 자체는 Task 5에서 이미 되므로 실제로는 instance B가 메시지를 받을 수도 있다. 이 테스트가 진짜로 검증하려는 배선(캐시 데코레이터가 실제로 살아있는 컴포넌트로 등록되어 있는지)은 다음 스텝에서 `ReferenceDataCacheConfig`를 추가해야 완전해지므로, 이 시점에서는 일단 실행해 현재 상태를 확인만 하고 다음 스텝으로 진행한다.

- [ ] **Step 4: ReferenceDataCacheConfig 작성**

```kotlin
package com.seaotter.triggerly.bootstrap.cache

import com.seaotter.triggerly.adapter.messaging.redis.EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL
import com.seaotter.triggerly.adapter.messaging.redis.WORKFLOW_CACHE_INVALIDATION_CHANNEL
import com.seaotter.triggerly.adapter.persistence.jpa.EventDefinitionPersistenceAdapter
import com.seaotter.triggerly.adapter.persistence.jpa.WorkflowPersistenceAdapter
import com.seaotter.triggerly.application.cache.CachedEventDefinitionAdapter
import com.seaotter.triggerly.application.cache.CachedWorkflowRepositoryAdapter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer

// EventDefinitionPersistenceAdapter/WorkflowPersistenceAdapter를 감싸 Caffeine으로 캐싱하는 데코레이터를
// @Primary로 등록한다 - EventIngestionController/IngestEventUseCase/TenantAwareTimeoutPoller는 여전히
// EventDefinitionPort/WorkflowRepositoryPort 인터페이스에만 의존하므로 이 배선을 전혀 몰라도 된다.
// 다른 앱 인스턴스에서 발행된 무효화 메시지를 받으려면 RedisMessageListenerContainer로 두 채널을 구독해
// 해당 데코레이터의 evict를 직접 호출한다 - 발행 인스턴스 자신도 같은 채널을 구독하므로 별도 로컬 경로
// 없이 항상 이 경로 하나로만 캐시가 비워진다.
@Configuration
class ReferenceDataCacheConfig {

  @Bean
  @Primary
  fun cachedEventDefinitionPort(delegate: EventDefinitionPersistenceAdapter): CachedEventDefinitionAdapter =
    CachedEventDefinitionAdapter(delegate)

  @Bean
  @Primary
  fun cachedWorkflowRepositoryPort(delegate: WorkflowPersistenceAdapter): CachedWorkflowRepositoryAdapter =
    CachedWorkflowRepositoryAdapter(delegate)

  @Bean
  fun cacheInvalidationListenerContainer(
    connectionFactory: RedisConnectionFactory,
    eventDefinitionCache: CachedEventDefinitionAdapter,
    workflowCache: CachedWorkflowRepositoryAdapter,
  ): RedisMessageListenerContainer {
    val container = RedisMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.addMessageListener(
      MessageListener { message, _ ->
        val (tenantId, code) = String(message.body).split(":", limit = 2)
        eventDefinitionCache.evict(tenantId, code)
      },
      ChannelTopic(EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL),
    )
    container.addMessageListener(
      MessageListener { message, _ ->
        val (tenantId, workflowId) = String(message.body).split(":", limit = 2)
        workflowCache.evict(tenantId, workflowId)
      },
      ChannelTopic(WORKFLOW_CACHE_INVALIDATION_CHANNEL),
    )
    return container
  }
}
```

- [ ] **Step 5: 테스트 재실행 (Docker 필요 - MySQL/ES/Kafka/Redis 전부 필요한 FullStackIntegrationTest)**

Run: `./gradlew :triggerly-bootstrap:test --tests "*ReferenceDataCacheCrossInstanceTest*"`
Expected: PASS

- [ ] **Step 6: 전체 컴파일 확인**

Run: `./gradlew compileKotlin compileTestKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 7: 커밋**

```bash
git add triggerly-bootstrap/build.gradle.kts \
  triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/cache/ReferenceDataCacheConfig.kt \
  triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/cache/ReferenceDataCacheCrossInstanceTest.kt
git commit -m "feat(bootstrap): 참조 데이터 캐시 데코레이터를 @Primary로 배선하고 Redis 무효화 채널 구독"
```

---

## Task 7: 멤버 쓰기 증폭 제거 (dirty tracking)

**Files:**
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCase.kt`
- Modify: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCaseTest.kt`

**Interfaces:**
- Consumes: 없음 (기존 `MemberCommandPort`/`RawEventMessage`/`MemberContext` 그대로 사용)
- Produces: `bulkResolveMembers`/`applyContext`의 동작 변경만 - 시그니처는 그대로(`bulkResolveMembers`는 여전히 `Map<Pair<String,String>, Member>` 반환)

- [ ] **Step 1: 실패하는 테스트 작성**

`IngestEventUseCaseTest.kt`에 아래 두 테스트를 추가한다:

```kotlin
  @Test
  fun `handleBatch는 기존 멤버의 컨텍스트가 실제로 변경되지 않으면 saveAll 대상에서 제외한다`() {
    val existing = Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1", email = "same@b.com")
    val messages = listOf(
      RawEventMessage(
        tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
        memberContext = MemberContext(tenantId = "t1", externalMemberId = "ext-1", email = "same@b.com"),
        attributes = null, occurredAt = LocalDateTime.now(),
      ),
    )
    every { memberCommandPort.findByExternalIds("t1", setOf("ext-1")) } returns listOf(existing)
    val savedInstancesSlot = slot<Collection<EventInstance>>()
    every { eventInstanceRepositoryPort.saveAll(capture(savedInstancesSlot)) } answers { savedInstancesSlot.captured.toList() }
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "LOGIN") } returns emptyList()

    useCase.handleBatch(messages)

    verify(exactly = 0) { memberCommandPort.saveAll(any()) }
  }

  @Test
  fun `handleBatch는 기존 멤버의 컨텍스트가 실제로 바뀌면 saveAll 대상에 포함한다`() {
    val existing = Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1", email = "old@b.com")
    val messages = listOf(
      RawEventMessage(
        tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
        memberContext = MemberContext(tenantId = "t1", externalMemberId = "ext-1", email = "new@b.com"),
        attributes = null, occurredAt = LocalDateTime.now(),
      ),
    )
    every { memberCommandPort.findByExternalIds("t1", setOf("ext-1")) } returns listOf(existing)
    val savedMembersSlot = slot<Collection<Member>>()
    every { memberCommandPort.saveAll(capture(savedMembersSlot)) } answers { savedMembersSlot.captured.toList() }
    val savedInstancesSlot = slot<Collection<EventInstance>>()
    every { eventInstanceRepositoryPort.saveAll(capture(savedInstancesSlot)) } answers { savedInstancesSlot.captured.toList() }
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "LOGIN") } returns emptyList()

    useCase.handleBatch(messages)

    verify(exactly = 1) { memberCommandPort.saveAll(any()) }
    assertEquals("new@b.com", savedMembersSlot.captured.first().email)
  }
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*IngestEventUseCaseTest*"`
Expected: FAIL - `기존 멤버의 컨텍스트가 실제로 변경되지 않으면 saveAll 대상에서 제외한다`가 실패(현재 코드는 컨텍스트 유무·변경 여부와 무관하게 항상 saveAll을 호출함)

- [ ] **Step 3: bulkResolveMembers/applyContext를 dirty-tracking하도록 수정**

`IngestEventUseCase.kt`에서 `bulkResolveMembers`와 `applyContext` 메서드 전체를 아래로 교체:

```kotlin
  // tenantId 단위로 findByExternalIds(IN절) 1회 + saveAll 1회로 멤버를 일괄 조회/갱신/생성한다.
  // 같은 배치 안에 동일한 (tenantId, externalMemberId)가 여러 번 오면 컨텍스트를 순서대로 누적 적용하고
  // 마지막 상태 1건만 저장 대상으로 남긴다. saveAll에는 "신규 생성된 멤버"와 "이번 배치에서 실제로 필드값이
  // 바뀐 기존 멤버"만 담는다 - applyContext가 실제 변경 여부를 반환하므로, 컨텍스트가 없거나(순수 트리거성
  // 이벤트) 값이 이미 동일한 이벤트는 멤버 UPDATE 자체를 스킵해 쓰기 증폭을 없앤다.
  private fun bulkResolveMembers(messages: List<RawEventMessage>): Map<Pair<String, String>, Member> {
    val existingByKey = mutableMapOf<Pair<String, String>, Member>()
    messages.filter { it.externalMemberId != null }.groupBy { it.tenantId }.forEach { (tenantId, msgs) ->
      val externalIds = msgs.mapNotNull { it.externalMemberId }.toSet()
      memberCommandPort.findByExternalIds(tenantId, externalIds)
        .forEach { existingByKey[tenantId to it.externalMemberId] = it }
    }

    val resolved = LinkedHashMap<Pair<String, String>, Member>()
    val toPersist = LinkedHashMap<Pair<String, String>, Member>()
    messages.forEach { message ->
      val externalId = message.externalMemberId ?: return@forEach
      val key = message.tenantId to externalId
      val member = resolved[key] ?: existingByKey[key]
      if (member != null) {
        val changed = message.memberContext?.let { applyContext(member, it) } ?: false
        resolved[key] = member
        // 같은 배치 안에서 이 멤버가 이미 한 번이라도 변경됐다면(toPersist에 이미 있음), 이번 이벤트가
        // 변경이 없더라도 저장 대상에서 빠지면 안 된다 - "한 번이라도 바뀌면 이번 배치는 저장" 규칙.
        if (changed || toPersist.containsKey(key)) toPersist[key] = member
      } else {
        val created = newMember(message.tenantId, externalId, message.memberContext)
        resolved[key] = created
        toPersist[key] = created
      }
    }
    if (toPersist.isEmpty()) return resolved

    val saved = memberCommandPort.saveAll(toPersist.values).associateBy { it.tenantId to it.externalMemberId }
    return resolved.mapValues { (key, member) -> saved[key] ?: member }
  }
```

```kotlin
  // 필드별로 실제 값이 달라질 때만 갱신하고, 하나라도 바뀌었으면 true를 반환한다 - bulkResolveMembers가
  // 이 반환값으로 "이 멤버를 이번 배치에서 실제로 저장해야 하는지"를 판단한다.
  private fun applyContext(member: Member, context: MemberContext): Boolean {
    var changed = false
    context.email?.let { if (member.email != it) { member.email = it; changed = true } }
    context.telephone?.let { if (member.telephone != it) { member.telephone = it; changed = true } }
    context.devicePlatform?.let { if (member.devicePlatform != it) { member.devicePlatform = it; changed = true } }
    context.birthday?.let { if (member.birthday != it) { member.birthday = it; changed = true } }
    context.status?.let { if (member.status != it) { member.status = it; changed = true } }
    context.lastLoginAt?.let { if (member.lastLoginAt != it) { member.lastLoginAt = it; changed = true } }
    return changed
  }
```

- [ ] **Step 4: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*IngestEventUseCaseTest*"`
Expected: PASS (기존 "handleBatch는 같은 배치 안의 동일 externalMemberId를..." 테스트도 여전히 PASS해야 한다 - 신규 멤버 생성 케이스라 dirty-tracking과 무관하게 항상 저장됨)

- [ ] **Step 5: 커밋**

```bash
git add triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCase.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCaseTest.kt
git commit -m "perf(ingest): 변경 없는 멤버는 saveAll 대상에서 제외해 쓰기 증폭 제거"
```

---

## Task 8: 대기 인스턴스 N+1 배치화

**Files:**
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCase.kt`
- Modify: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCaseTest.kt`

**Interfaces:**
- Consumes: `WorkflowInstanceRepositoryPort.findAllById(ids: Collection<String>): List<WorkflowInstance>` (Task 1)
- Produces: `handleBatch`의 동작 변경만 - 시그니처/외부 계약은 그대로

- [ ] **Step 1: 실패하는 테스트 작성**

`IngestEventUseCaseTest.kt`에 추가:

```kotlin
  @Test
  fun `handleBatch는 배치 안에 대기 인스턴스 매칭 후보가 여러 건이어도 findAllById를 1번만 호출한다`() {
    val member = Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1")
    val messages = listOf(
      RawEventMessage(
        tenantId = "t1", eventCode = "PAY_DONE", externalMemberId = "ext-1",
        memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      ),
      RawEventMessage(
        tenantId = "t1", eventCode = "PAY_DONE", externalMemberId = "ext-1",
        memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      ),
    )
    every { memberCommandPort.findByExternalIds("t1", setOf("ext-1")) } returns listOf(member)
    every { waitingIndexPort.lookup("t1", "PAY_DONE", "m1") } returns listOf("instance-1", "instance-2")
    val waitingInstance1 = mockk<WorkflowInstance>()
    val waitingInstance2 = mockk<WorkflowInstance>()
    every { waitingInstance1.workflowId } returns "wf-1"
    every { waitingInstance2.workflowId } returns "wf-1"
    every { workflowInstanceRepositoryPort.findAllById(listOf("instance-1", "instance-2")) } returns
      listOf(waitingInstance1, waitingInstance2)
    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findById("t1", "wf-1") } returns workflow
    every { workflowEngine.resumeOnMatch(any(), workflow, any()) } returns mockk()
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "PAY_DONE") } returns emptyList()
    every { eventInstanceRepositoryPort.saveAll(any()) } answers { firstArg<Collection<EventInstance>>().toList() }

    useCase.handleBatch(messages)

    verify(exactly = 1) { workflowInstanceRepositoryPort.findAllById(listOf("instance-1", "instance-2")) }
    verify(exactly = 0) { workflowInstanceRepositoryPort.findById(any()) }
    verify(exactly = 2) { workflowEngine.resumeOnMatch(any(), workflow, any()) }
  }
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*IngestEventUseCaseTest*"`
Expected: FAIL - `findAllById(any())`가 호출되지 않고 `findById`가 메시지당 개별 호출됨(현재 구현)

- [ ] **Step 3: handleBatch를 2-패스로 재구성**

`IngestEventUseCase.kt`의 `handleBatch` 메서드 전체를 아래로 교체(주석의 재시도/멱등 설명은 유지하고, 대기 인스턴스 조회 부분만 2-패스로 바뀐다):

```kotlin
  // Kafka 컨슈머(WorkflowTriggerConsumer)가 한 번의 poll로 끌어온 배치를 통째로 넘기는 진입점.
  // 트래픽이 적을 때는 배치 크기가 1~2건이라 handle()과 사실상 동일하게 동작하고, 트래픽이 몰릴 때는
  // 멤버 조회/저장을 (tenantId) 단위로 묶어 DB 왕복 횟수를 줄인다.
  // 워크플로 상태 전이(start/resumeOnMatch)는 이벤트마다 결과가 달라 배치화할 수 없으므로 그대로 순회한다.
  //
  // [대기 인스턴스 N+1 제거] 배치를 순회하기 전에 먼저 각 메시지가 매칭할 수 있는 대기 인스턴스 후보
  // id를 waitingIndexPort.lookup(Redis, 메시지당 유지)으로 모두 모으고, workflowInstanceRepositoryPort.
  // findAllById로 그 전체를 IN절 1회에 조회한다. 본 순회에서는 이 사전 조회 결과만 참조하므로 메시지마다
  // 개별 findById가 나가지 않는다.
  //
  // [재시도/멱등 처리] 같은 배치가 실패해서 카프카가 다시 배달하더라도(레코드는 그대로, RawEventMessage.eventId도
  // 그대로) 이미 액션(쿠폰 발급 등)까지 실행된 이벤트를 또 실행하면 안 된다. 그래서:
  //   1) 배치 시작 시 eventId가 이미 EventInstance에 저장돼 있는 건("이전 시도에서 엔진 실행까지 끝난 이벤트")
  //      찾아서 통째로 스킵한다.
  //   2) 이벤트 하나의 엔진 실행이 "성공적으로 끝난 뒤에만" EventInstance를 저장 대상에 담는다 - 실행 도중
  //      실패하면 이 이벤트는 저장되지 않으므로, 재시도 때 다시 (1)에 안 걸리고 처음부터 재실행된다.
  //   3) 레코드 하나가 실패하면 그 인덱스까지는 이미 성공했으니 먼저 커밋(saveAll)해두고,
  //      실패한 인덱스를 BatchEventProcessingException으로 알려서 그 레코드부터만 재시도/DLT 대상이 되게 한다
  //      (WorkflowTriggerConsumer가 이걸 BatchListenerFailedException으로 바꿔 카프카 컨테이너에 전달한다).
  //   4) 한 이벤트가 워크플로 여러 개를 트리거하는데 그중 하나가 실패해도, 앞서 이미 끝낸 워크플로들은
  //      재시도 때 다시 실행되지 않는다 - WorkflowEngine.start()가 인스턴스 id를 eventId+workflowId로 고정해
  //      이미 있는 인스턴스를 그대로 반환하기 때문(WorkflowEngine.kt 참고).
  //   5) 대기 인스턴스 사전 조회(findAllById 포함)는 어떤 레코드도 실제로 처리(엔진 실행)하지 않으므로,
  //      여기서 예외가 나면(예: Redis/DB 장애) 배치 전체를 그대로 재시도해도 안전하다.
  fun handleBatch(messages: List<RawEventMessage>) {
    val regularMessages = messages.filter { it.syntheticTimeoutForInstanceId == null }
    val alreadyProcessedEventIds = eventInstanceRepositoryPort.findExistingIds(regularMessages.map { it.eventId })
    val pendingMessages = regularMessages.filterNot { it.eventId in alreadyProcessedEventIds }
    val resolvedMembers = bulkResolveMembers(pendingMessages)

    val candidateInstanceIdsByMessage = HashMap<RawEventMessage, List<String>>()
    messages.forEach { message ->
      if (message.syntheticTimeoutForInstanceId != null) return@forEach
      if (message.eventId in alreadyProcessedEventIds) return@forEach
      val member = message.externalMemberId?.let { resolvedMembers[message.tenantId to it] } ?: return@forEach
      candidateInstanceIdsByMessage[message] = waitingIndexPort.lookup(message.tenantId, message.eventCode, member.id)
    }
    val allCandidateIds = candidateInstanceIdsByMessage.values.flatten().distinct()
    val waitingInstancesById = workflowInstanceRepositoryPort.findAllById(allCandidateIds).associateBy { it.id }

    val workflowCache = mutableMapOf<Pair<String, String>, List<Workflow>>()
    val processedInstances = mutableListOf<EventInstance>()

    messages.forEachIndexed { index, message ->
      try {
        if (message.syntheticTimeoutForInstanceId != null) {
          // 타임아웃 이벤트는 handleTimeout 안의 "인스턴스 상태가 WAITING일 때만" 가드로 이미 멱등하다
          // (같은 인스턴스가 이미 처리됐으면 상태가 바뀌어 있어 자연히 재실행되지 않음). eventId 판별 불필요.
          handleTimeout(message.syntheticTimeoutForInstanceId)
          return@forEachIndexed
        }
        if (message.eventId in alreadyProcessedEventIds) {
          log.debug("이미 처리된 이벤트 재배달 - 스킵: eventId={}", message.eventId)
          return@forEachIndexed
        }

        val member = message.externalMemberId?.let { resolvedMembers[message.tenantId to it] }
        val context = buildContext(message, member)

        workflowCache.getOrPut(message.tenantId to message.eventCode) {
          workflowRepositoryPort.findEnabledByTriggerEventCode(message.tenantId, message.eventCode)
        }.forEach { workflow -> workflowEngine.start(workflow, message.tenantId, member?.id, context, message.eventId) }

        candidateInstanceIdsByMessage[message].orEmpty()
          .mapNotNull { waitingInstancesById[it] }
          .forEach { waiting ->
            val workflow = workflowRepositoryPort.findById(message.tenantId, waiting.workflowId) ?: return@forEach
            workflowEngine.resumeOnMatch(waiting, workflow, context)
          }

        processedInstances += EventInstance(
          id = message.eventId,
          tenantId = message.tenantId,
          eventCode = message.eventCode,
          occurredAt = message.occurredAt,
          memberId = member?.id,
          attributes = message.attributes,
        )
      } catch (ex: Exception) {
        if (processedInstances.isNotEmpty()) eventInstanceRepositoryPort.saveAll(processedInstances)
        throw BatchEventProcessingException(index, message.eventId, ex)
      }
    }

    if (processedInstances.isNotEmpty()) eventInstanceRepositoryPort.saveAll(processedInstances)
  }
```

`RawEventMessage`가 `data class`라 `HashMap<RawEventMessage, List<String>>`의 키로 쓰려면 `equals`/`hashCode`가 필요한데, `RawEventMessage`는 이미 `data class`라 구조적 동등성이 자동 생성되어 있다 - 같은 배치 안에 완전히 동일한 필드값(`eventId` 포함)을 가진 메시지가 두 개 존재할 수는 없으므로(각 메시지의 `eventId`가 서로 다른 UUID) 키 충돌 걱정은 없다.

- [ ] **Step 4: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*IngestEventUseCaseTest*"`
Expected: PASS (Task 7에서 추가한 테스트들 포함, 전체 PASS)

- [ ] **Step 5: 커밋**

```bash
git add triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCase.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCaseTest.kt
git commit -m "perf(ingest): 대기 인스턴스 매칭을 findAllById 벌크 조회로 배치화"
```

---

## Task 9: 타임아웃 폴러 재설계 (테넌트별 라운드로빈 + 공유 워커풀 + 세마포어 + 분산락)

**Files:**
- Delete: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/WaitingInstanceTimeoutPoller.kt`
- Delete: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/WaitingInstanceTimeoutPollerTest.kt`
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/TenantAwareTimeoutPoller.kt`
- Test: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/TenantAwareTimeoutPollerTest.kt`
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowInstanceRepositoryPort.kt` (옛 `findWaitingExpired` 제거)
- Modify: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstanceJpaRepository.kt` (옛 쿼리 메서드 제거)
- Modify: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapter.kt` (옛 구현 제거)
- Modify: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapterTest.kt` (옛 테스트 제거)
- Modify: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/engine/WorkflowEngineTest.kt` (Fake의 옛 override 제거)
- Modify: `triggerly-bootstrap/src/main/resources/application.yml`

**Interfaces:**
- Consumes: `WorkflowRepositoryPort.findDistinctTenantIds()`(Task 2, `@Primary` 캐시 경유), `WorkflowInstanceRepositoryPort.findWaitingExpiredByTenant`(Task 1), `DistributedLockPort.tryLock`(Task 3), 기존 `EventPublisherPort.publish`

- [ ] **Step 1: application.yml에 신규 설정값 추가**

`triggerly-bootstrap/src/main/resources/application.yml`의 `triggerly.scheduler` 블록을 아래로 교체:

```yaml
  scheduler:
    timeout-poll-interval-ms: 10000
    timeout-poll-batch-size-per-tenant: 200
    timeout-poll-worker-pool-size: 20
    timeout-poll-per-tenant-concurrency: 4
    timeout-poll-lock-ttl-ms: 30000
    event-instance-retention-days: 30
```

- [ ] **Step 2: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertTrue

class TenantAwareTimeoutPollerTest {

  private val workflowInstanceRepositoryPort = mockk<WorkflowInstanceRepositoryPort>()
  private val workflowRepositoryPort = mockk<WorkflowRepositoryPort>()
  private val distributedLockPort = mockk<DistributedLockPort>()
  private val eventPublisherPort = mockk<EventPublisherPort>(relaxed = true)

  private fun poller(perTenantConcurrency: Int = 4, poolSize: Int = 20) =
    TenantAwareTimeoutPoller(
      workflowInstanceRepositoryPort, workflowRepositoryPort, distributedLockPort, eventPublisherPort,
      batchSizePerTenant = 200, workerPoolSize = poolSize, perTenantConcurrency = perTenantConcurrency, lockTtlMs = 30000,
    ).also { it.start() }

  private fun instance(id: String, tenantId: String) = WorkflowInstance(
    id = id, workflowId = "wf-1", tenantId = tenantId, triggerEventCode = "CART_ADD",
    version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
  )

  @Test
  fun `테넌트가 없으면 아무 것도 조회하지 않는다`() {
    every { workflowRepositoryPort.findDistinctTenantIds() } returns emptySet()
    val sut = poller()

    sut.pollExpiredInstances()

    verify(exactly = 0) { workflowInstanceRepositoryPort.findWaitingExpiredByTenant(any(), any(), any()) }
    sut.stop()
  }

  @Test
  fun `락을 선점하면 타임아웃 합성 이벤트를 발행하고, 이미 잠겨있으면 스킵한다`() {
    every { workflowRepositoryPort.findDistinctTenantIds() } returns setOf("t1")
    every { workflowInstanceRepositoryPort.findWaitingExpiredByTenant("t1", any(), any()) } returns
      listOf(instance("i1", "t1"), instance("i2", "t1"))
    every { distributedLockPort.tryLock("timeout-lock:i1", any()) } returns true
    every { distributedLockPort.tryLock("timeout-lock:i2", any()) } returns false
    val sut = poller()

    sut.pollExpiredInstances()

    verify(exactly = 1) { eventPublisherPort.publish(match { it.syntheticTimeoutForInstanceId == "i1" }) }
    verify(exactly = 0) { eventPublisherPort.publish(match { it.syntheticTimeoutForInstanceId == "i2" }) }
    sut.stop()
  }

  @Test
  fun `한 테넌트의 동시 처리 건수는 설정된 per-tenant-concurrency를 넘지 않는다`() {
    val inFlight = AtomicInteger(0)
    val maxObserved = AtomicInteger(0)
    every { workflowRepositoryPort.findDistinctTenantIds() } returns setOf("busy-tenant")
    every { workflowInstanceRepositoryPort.findWaitingExpiredByTenant("busy-tenant", any(), any()) } returns
      (1..50).map { instance("i$it", "busy-tenant") }
    every { distributedLockPort.tryLock(any(), any()) } answers {
      val current = inFlight.incrementAndGet()
      maxObserved.updateAndGet { prev -> maxOf(prev, current) }
      Thread.sleep(20)
      inFlight.decrementAndGet()
      true
    }
    val sut = poller(perTenantConcurrency = 4, poolSize = 20)

    sut.pollExpiredInstances()

    assertTrue(
      maxObserved.get() in 2..4,
      "관측된 최대 동시 처리 건수(${maxObserved.get()})는 설정값(4) 이하이면서 실제 동시성이 있었어야 한다",
    )
    sut.stop()
  }
}
```

- [ ] **Step 3: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-bootstrap:test --tests "*TenantAwareTimeoutPollerTest*"`
Expected: FAIL - `Unresolved reference: TenantAwareTimeoutPoller`

- [ ] **Step 4: TenantAwareTimeoutPoller 구현**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.Semaphore

// 스펙 6절 4단계: WAITING인데 waitingUntil이 지난 인스턴스를 찾아 타임아웃 합성 이벤트를 같은 Kafka
// 토픽에 다시 produce한다. 여러 앱 인스턴스가 리더 선출 없이 매 틱 동시에 폴링한다는 전제로:
//  1) 테넌트별로 만료 인스턴스를 조회하고(findWaitingExpiredByTenant, (tenant_id, status, waiting_until)
//     인덱스 사용), 제출 순서를 테넌트별 라운드로빈으로 인터리빙해 특정 테넌트의 백로그가 다른 테넌트를
//     굶기지 않게 한다.
//  2) 스레드 수는 테넌트 수와 무관하게 고정 크기 워커 풀 하나를 전 테넌트가 공유한다. 테넌트별
//     Semaphore로 "이 풀에서 동시에 처리 중인 이 테넌트의 작업 수"만 제한한다 - 테넌트가 늘어도
//     세마포어(카운터)만 늘 뿐 스레드/DB 커넥션은 늘지 않는다.
//  3) 여러 인스턴스가 같은 만료 row를 동시에 집었을 때는 Redis 행 단위 락(tryLock, TTL 경과 시 자동 해제)을
//     선점한 쪽만 실제로 합성 이벤트를 발행한다 - 실패하면 스킵한다(다른 인스턴스가 이미 처리했으므로
//     유실이 아니다).
@Component
class TenantAwareTimeoutPoller(
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val workflowRepositoryPort: WorkflowRepositoryPort,
  private val distributedLockPort: DistributedLockPort,
  private val eventPublisherPort: EventPublisherPort,
  @Value("\${triggerly.scheduler.timeout-poll-batch-size-per-tenant:200}") private val batchSizePerTenant: Int,
  @Value("\${triggerly.scheduler.timeout-poll-worker-pool-size:20}") private val workerPoolSize: Int,
  @Value("\${triggerly.scheduler.timeout-poll-per-tenant-concurrency:4}") private val perTenantConcurrency: Int,
  @Value("\${triggerly.scheduler.timeout-poll-lock-ttl-ms:30000}") private val lockTtlMs: Long,
) {
  private val log = LoggerFactory.getLogger(TenantAwareTimeoutPoller::class.java)
  private lateinit var workerPool: ExecutorService
  private val perTenantSemaphores = ConcurrentHashMap<String, Semaphore>()

  @PostConstruct
  fun start() {
    workerPool = Executors.newFixedThreadPool(workerPoolSize)
  }

  @PreDestroy
  fun stop() {
    workerPool.shutdown()
  }

  @Scheduled(fixedDelayString = "\${triggerly.scheduler.timeout-poll-interval-ms:10000}")
  fun pollExpiredInstances() {
    val tenantIds = workflowRepositoryPort.findDistinctTenantIds()
    if (tenantIds.isEmpty()) return

    val now = LocalDateTime.now()
    val backlogs = tenantIds
      .associateWith { tenantId -> workflowInstanceRepositoryPort.findWaitingExpiredByTenant(tenantId, now, batchSizePerTenant) }
      .filterValues { it.isNotEmpty() }
    if (backlogs.isEmpty()) return

    val futures = interleave(backlogs).map { instance -> workerPool.submit { processExpiredInstance(instance) } }
    waitForCompletion(futures)
  }

  // 테넌트1에서 1건, 테넌트2에서 1건, ... 한 바퀴 돌고 다시 테넌트1 - 공유 워커풀이 포화 상태여도
  // 뒷순번 테넌트가 굶지 않도록 제출 순서 자체를 섞는다.
  private fun interleave(backlogs: Map<String, List<WorkflowInstance>>): List<WorkflowInstance> {
    val queues = backlogs.values.map { it.toMutableList() }.filter { it.isNotEmpty() }.toMutableList()
    val result = mutableListOf<WorkflowInstance>()
    while (queues.isNotEmpty()) {
      val iterator = queues.iterator()
      while (iterator.hasNext()) {
        val queue = iterator.next()
        result += queue.removeAt(0)
        if (queue.isEmpty()) iterator.remove()
      }
    }
    return result
  }

  private fun processExpiredInstance(instance: WorkflowInstance) {
    val semaphore = perTenantSemaphores.computeIfAbsent(instance.tenantId) { Semaphore(perTenantConcurrency) }
    semaphore.acquire()
    try {
      if (!distributedLockPort.tryLock("timeout-lock:${instance.id}", Duration.ofMillis(lockTtlMs))) {
        return // 다른 인스턴스가 이미 처리 중 - 스킵(유실 아님, 처리 안 되면 다음 틱까지 그대로 남아있음)
      }
      log.info("타임아웃 감지: instanceId=${instance.id} tenantId=${instance.tenantId} node=${instance.currentNodeId}")
      eventPublisherPort.publish(
        RawEventMessage(
          tenantId = instance.tenantId,
          eventCode = "__TIMEOUT__",
          externalMemberId = null,
          memberContext = null,
          attributes = null,
          occurredAt = LocalDateTime.now(),
          syntheticTimeoutForInstanceId = instance.id,
        ),
      )
    } finally {
      semaphore.release()
    }
  }

  private fun waitForCompletion(futures: List<Future<*>>) {
    futures.forEach { future ->
      try {
        future.get()
      } catch (e: Exception) {
        log.error("타임아웃 인스턴스 처리 중 예외 발생", e)
      }
    }
  }
}
```

- [ ] **Step 5: 옛 파일/메서드 제거**

```bash
rm triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/WaitingInstanceTimeoutPoller.kt
rm triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/WaitingInstanceTimeoutPollerTest.kt
```

`WorkflowInstanceRepositoryPort.kt`에서 `fun findWaitingExpired(now: LocalDateTime, limit: Int = 200): List<WorkflowInstance>` 줄을 제거한다.

`WorkflowInstanceJpaRepository.kt`에서 `findAllByStatusAndWaitingUntilLessThanEqual` 메서드 선언을 제거하고 `import org.springframework.data.domain.Pageable`이 여전히 `findAllByTenantIdAndStatusAndWaitingUntilLessThanEqualOrderByWaitingUntilAsc`에 쓰이므로 import는 유지한다.

`WorkflowInstancePersistenceAdapter.kt`에서 `override fun findWaitingExpired(...)` 메서드 전체를 제거한다.

`WorkflowInstancePersistenceAdapterTest.kt`에서 `findWaitingExpired는 waitingUntil이 지난 WAITING 인스턴스만 반환한다` 테스트 메서드를 제거한다(Task 1에서 추가한 `findWaitingExpiredByTenant` 테스트가 이를 대체한다).

`WorkflowEngineTest.kt`의 `FakeWorkflowInstanceRepository`에서 `override fun findWaitingExpired(now: LocalDateTime, limit: Int) = ...` 줄을 제거한다(Task 1에서 추가한 `findWaitingExpiredByTenant`/`findAllById` override만 남긴다).

- [ ] **Step 6: 테스트 재실행**

Run: `./gradlew :triggerly-bootstrap:test --tests "*TenantAwareTimeoutPollerTest*"`
Expected: PASS

Run: `./gradlew compileKotlin compileTestKotlin`
Expected: BUILD SUCCESSFUL (옛 메서드 제거로 인한 컴파일 에러가 없어야 한다)

- [ ] **Step 7: 커밋**

```bash
git add -A triggerly-bootstrap triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowInstanceRepositoryPort.kt \
  triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstanceJpaRepository.kt \
  triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapter.kt \
  triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapterTest.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/engine/WorkflowEngineTest.kt
git commit -m "perf(scheduler): 타임아웃 폴러를 테넌트별 라운드로빈+공유워커풀+세마포어+분산락으로 재설계"
```

---

## Task 10: 액션 디스패치 멱등성 가드

**Files:**
- Modify: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/DispatchActionUseCase.kt`
- Modify: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/DispatchActionUseCaseTest.kt`

**Interfaces:**
- Consumes: `DistributedLockPort.tryLock`(Task 3)
- Produces: `handle`의 동작 변경만 - 시그니처는 그대로

- [ ] **Step 1: 실패하는 테스트 작성**

`DispatchActionUseCaseTest.kt` 전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.domain.ActionDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class DispatchActionUseCaseTest {

  private val actionExecutor = mockk<ActionExecutor>(relaxed = true)
  private val distributedLockPort = mockk<DistributedLockPort>()
  private val useCase = DispatchActionUseCase(actionExecutor, distributedLockPort)

  private fun message(dispatchId: String) = ActionDispatchMessage(
    tenantId = "t1", memberId = "m1", action = ActionDefinition.IssueCoupon("BIRTHDAY10"),
    workflowInstanceId = "wf-instance-1", nodeId = "n3", dispatchId = dispatchId,
  )

  @Test
  fun `dedup 락을 처음 선점하면 ActionExecutor를 호출한다`() {
    every { distributedLockPort.tryLock("dispatch-dedup:dispatch-1", any()) } returns true

    useCase.handle(message("dispatch-1"))

    verify(exactly = 1) { actionExecutor.execute(any()) }
  }

  @Test
  fun `dedup 락을 이미 선점한 상태면 ActionExecutor를 호출하지 않는다`() {
    every { distributedLockPort.tryLock("dispatch-dedup:dispatch-1", any()) } returns false

    useCase.handle(message("dispatch-1"))

    verify(exactly = 0) { actionExecutor.execute(any()) }
  }
}
```

- [ ] **Step 2: 테스트 실행 → 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "*DispatchActionUseCaseTest*"`
Expected: FAIL - 생성자 인자 개수 불일치 컴파일 에러

- [ ] **Step 3: DispatchActionUseCase 수정**

전체를 아래로 교체:

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.DistributedLockPort
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

// ActionDispatchConsumer(adapter-messaging)의 진입점. WorkflowEngine의 raw-events 처리 경로와 완전히
// 분리된 컨슈머 그룹에서 호출되므로, 여기서 프로바이더 API가 느려지거나 실패해도 이벤트 수집 쪽 스레드는
// 영향받지 않는다. fire-and-forget이라 결과를 어디에도 다시 쓰지 않는다 - 실패하면 예외를 그대로 던져
// ActionDispatchConsumer 쪽 재시도/DLT가 처리하게 둔다.
//
// dispatchId는 WorkflowEngine이 (workflowInstanceId, nodeId)로 결정론적으로 만들어 재시도 때도 항상
// 같은 값이다(WorkflowEngine.kt 참고) - 그런데도 지금까지 이 값을 실제로 검사하는 곳이 없어, 카프카
// 재배달이나 타임아웃 폴러의 락 TTL 만료 후 재획득 같은 엣지케이스에서 실제 액션(쿠폰 발급 등)이 중복
// 실행될 수 있었다. DistributedLockPort.tryLock을 24시간 TTL로 걸어 "이 dispatchId를 이미 처리했다"는
// 표시로 재사용한다 - 타임아웃 폴러의 행 단위 락(짧은 TTL)과 같은 메커니즘을 더 긴 TTL로 쓰는 것뿐이다.
@Service
class DispatchActionUseCase(
  private val actionExecutor: ActionExecutor,
  private val distributedLockPort: DistributedLockPort,
) {
  private val log = LoggerFactory.getLogger(DispatchActionUseCase::class.java)

  fun handle(message: ActionDispatchMessage) {
    if (!distributedLockPort.tryLock("dispatch-dedup:${message.dispatchId}", DEDUP_TTL)) {
      log.info("이미 처리된 디스패치 - 스킵: dispatchId={}", message.dispatchId)
      return
    }
    actionExecutor.execute(message.action)
  }

  companion object {
    private val DEDUP_TTL: Duration = Duration.ofHours(24)
  }
}
```

- [ ] **Step 4: 테스트 재실행**

Run: `./gradlew :triggerly-application:test --tests "*DispatchActionUseCaseTest*"`
Expected: PASS

- [ ] **Step 5: 전체 테스트 스위트 최종 확인**

Run: `./gradlew compileKotlin compileTestKotlin test` (Docker 필요한 Testcontainers 통합 테스트 포함 - 로컬에 Docker가 없다면 최소 `./gradlew :triggerly-domain:test :triggerly-application:test`로 순수 단위 테스트만이라도 확인)
Expected: BUILD SUCCESSFUL

- [ ] **Step 6: 커밋**

```bash
git add triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/DispatchActionUseCase.kt \
  triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/DispatchActionUseCaseTest.kt
git commit -m "feat(dispatch): DistributedLockPort 기반 액션 디스패치 멱등성 가드 추가"
```

---

## 셀프 리뷰 결과

- **스펙 커버리지**: 스펙의 5개 항목(타임아웃 폴러 격리→Task 1/3/9, 참조 데이터 캐시→Task 2/4/5/6, 멤버 쓰기 증폭→Task 7, N+1 배치화→Task 1/8, 디스패치 멱등성→Task 3/10) 모두 태스크가 존재한다. 스펙의 신규 인덱스(Task 1), 설정값(Task 9), Redis fail-open 정책(Global Constraints에 명시, 각 어댑터가 예외를 던지지 않고 boolean으로 결과를 표현하는 설계 자체가 이를 만족)도 반영됐다.
- **플레이스홀더 스캔**: TBD/TODO 없음, 모든 스텝에 실제 코드 포함.
- **타입 일관성**: `DistributedLockPort.tryLock(key, ttl)`이 Task 3/9/10에서 동일 시그니처로 쓰인다. `CacheInvalidationPort.publish(topic, key)`가 Task 5에서 정의되고 Task 5 자신의 usecase 수정에서만 쓰이며 Task 6은 채널 상수만 참조한다(일관됨). `WorkflowInstanceRepositoryPort.findAllById`/`findWaitingExpiredByTenant`가 Task 1에서 정의된 시그니처 그대로 Task 8/9에서 쓰인다.
- **태스크 간 파일 충돌**: Task 7과 Task 8은 둘 다 `IngestEventUseCase.kt`를 수정하지만 각각 `bulkResolveMembers`/`applyContext`(Task 7)와 `handleBatch`(Task 8)로 메서드가 분리되어 있고, Task 8의 전체 교체 코드는 Task 7이 반영된 이후의 파일 상태를 전제로 작성되어 순서대로 적용하면 충돌이 없다.
