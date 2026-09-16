# 수집/실행 파이프라인 성능 개선 설계

## 목표

triggerly-claude를 대형 고객사(10곳, 각 100만+ 회원) 규모로 상용화하기 위해, 현재 수집(ingest)~워크플로 실행 경로에 남아있는 DB 병목을 제거하고 수평 확장이 실제로 처리량 증가로 이어지는 구조를 만든다.

## 스코프 아닌 것

멀티테넌시 인증/인가/저장소 격리는 별도 관심사이며 이 설계에 포함하지 않는다. 단, 타임아웃 폴러의 "테넌트 하나의 병목이 다른 테넌트에 영향 주지 않는다"는 요구는 이 설계의 핵심 목표이므로 포함한다(자원 격리이지 인증/격리 전체 스펙은 아님).

## 현재 병목 (코드 근거)

1. **EventDefinition 조회**: `EventIngestionController.ingest()`가 매 HTTP 요청마다 `eventDefinitionPort.findByTenantAndCode()`를 캐시 없이 호출 (`EventIngestionController.kt:28`).
2. **멤버 쓰기 증폭**: `IngestEventUseCase.bulkResolveMembers()`가 컨텍스트 변경 여부와 무관하게 배치 내 모든 기존 멤버를 `saveAll`에 담아 매번 UPDATE (`IngestEventUseCase.kt:133-156`).
3. **대기 인스턴스 매칭 N+1**: `handleBatch()` 안에서 이벤트마다 `waitingIndexPort.lookup()` → `workflowInstanceRepositoryPort.findById()`(개별) → `workflowRepositoryPort.findById()`(개별)를 반복 (`IngestEventUseCase.kt:104-110`).
4. **타임아웃 폴러 글로벌 상한**: `WaitingInstanceTimeoutPoller`가 `fixedDelay=10000ms` + `findWaitingExpired(limit=200)`로 전 테넌트 통틀어 20건/초 고정 상한, `Sort` 없어 처리 순서 미보장, 테넌트 격리 없음 (`WaitingInstanceTimeoutPoller.kt:20-22`, `WorkflowInstanceRepositoryPort.kt:9`).
5. **액션 디스패치 멱등성 부재**: `dispatchId`가 결정론적으로 생성되지만(`WorkflowEngine.kt:119`) 소비 시점(`DispatchActionUseCase.handle()`)에서 중복 체크가 전혀 없음 — 카프카 재배달이나 타임아웃 중복 발행 시 실제로 액션이 두 번 실행될 수 있음.

## 설계

### 1. 분산 타임아웃 폴러 (테넌트별 라운드로빈 + 공유 풀 + 행 단위 분산락)

**동작 흐름 (매 틱, 모든 앱 인스턴스가 동시에 실행 — 리더 선출 없음)**

1. 인메모리에 캐시된 "활성 테넌트 목록"(아래 4번 캐시 계층에서 제공, TTL 60초 또는 워크플로 생성 시 무효화)을 순회 대상으로 삼는다. `SELECT DISTINCT tenant_id FROM workflow`를 소스로 하며, `workflow_instance`가 아니라 훨씬 작고 안정적인 `workflow` 테이블을 스캔하므로 저비용이다.
2. 테넌트별로 `SELECT * FROM workflow_instance WHERE tenant_id=? AND status='WAITING' AND waiting_until<=? ORDER BY waiting_until ASC LIMIT ?`를 실행한다. 신규 복합 인덱스 `(tenant_id, status, waiting_until)`가 필터+정렬을 모두 커버한다.
3. 이번 틱에 제출할 작업 목록은 **테넌트별로 라운드로빈 인터리빙**해서 만든다(테넌트1에서 1건, 테넌트2에서 1건, … 순환). 공유 풀이 포화 상태여도 특정 테넌트가 뒷순번이라 굶는 일이 없다.
4. 작업은 **고정 크기 공유 스레드 풀**(테넌트 수와 무관, 이 폴러 전용 Hikari 커넥션 풀 크기에 맞춰 설정)에 제출한다. `ConcurrentHashMap<tenantId, Semaphore>`로 테넌트당 동시 처리 슬롯 상한을 걸어, 한 테넌트의 백로그가 아무리 커도 공유 풀에서 자기 몫 이상을 못 가져가게 한다. 테넌트 수가 늘어도 세마포어(카운터)만 늘 뿐 스레드/커넥션은 그대로다.
5. 인스턴스 여러 대가 같은 만료 row를 동시에 집었을 경우를 위해, 합성 타임아웃 이벤트 발행 직전 Redis `SET timeout-lock:{instanceId} 1 NX PX {ttlMs}`로 tryLock한다. 실패하면(이미 다른 인스턴스가 처리 중) 스킵한다 — `OutboxResolver`의 tryLock→처리→해제 패턴과 동일. TTL은 "합성 이벤트가 컨슘돼 인스턴스 상태가 WAITING을 벗어나기까지 걸리는 시간"보다 넉넉히 잡는다(기본 30초, 설정 가능).

**신규 설정값** (`triggerly.scheduler.*`):
- `timeout-poll-interval-ms` (기존 유지, 기본 10000)
- `timeout-poll-batch-size-per-tenant` (기본 200)
- `timeout-poll-worker-pool-size` (기본 20)
- `timeout-poll-per-tenant-concurrency` (기본 4)
- `timeout-poll-lock-ttl-ms` (기본 30000)
- `timeout-poll-dedicated-datasource` (별도 HikariCP 풀 사용 여부/크기 — 수집 API용 풀과 커넥션 경합 방지)

### 2. 참조 데이터 캐시 (EventDefinition + Workflow 정의 + 테넌트 목록)

`EventDefinition`, `Workflow` 정의, "테넌트 목록" 셋 다 관리자 CRUD로만 바뀌고 읽기가 압도적인 참조 데이터이므로 하나의 캐시 컴포넌트를 공유한다.

- **L1**: 인스턴스 로컬 Caffeine 캐시. 키: `(tenantId, eventCode) -> EventDefinition`, `(tenantId, workflowId) -> Workflow`, `"__tenants__" -> Set<tenantId>`.
- **즉시 무효화**: `ManageEventDefinitionUseCase`/`ManageWorkflowUseCase`의 저장·삭제 시점에 이미 쓰고 있는 `ApplicationEvent`(→`MemberSavedEvent`와 동일 컨벤션) 발행 후, 이를 구독하는 어댑터가 Redis Pub/Sub 채널(`cache:invalidate:event-definition`, `cache:invalidate:workflow`, `cache:invalidate:tenants`)로 브로드캐스트한다. 각 인스턴스는 구독자로 해당 키만 evict한다.
- **TTL 안전망**: 60초 — Pub/Sub 메시지 유실(네트워크 순단 등) 대비.
- 효과: `EventIngestionController.ingest()`의 매 요청 MySQL 조회, `handleBatch()` 안의 반복적인 워크플로 정의 조회, 타임아웃 폴러의 테넌트 목록 조회를 전부 이 캐시로 해결한다.

### 3. 멤버 쓰기 증폭 제거

`applyContext(member, context)`가 각 필드를 "실제로 값이 다를 때만" 갱신하고 **변경 여부(Boolean)를 반환**하도록 변경한다. `bulkResolveMembers`는 다음 기준으로만 `saveAll` 대상에 포함한다:
- 신규 생성 멤버 (항상 저장 필요)
- 이번 배치에서 `applyContext`가 `true`(실제 변경)를 반환한 기존 멤버

컨텍스트가 없거나(순수 트리거성 이벤트) 값이 이미 동일한 이벤트는 멤버 저장 자체가 스킵된다. 반환 맵(`Map<Pair<tenantId,externalId>, Member>`)은 저장 여부와 무관하게 이번 배치에서 resolve된 모든 멤버를 포함해야 하므로, 스킵된 멤버는 `existingByKey`에서 그대로 병합해 반환한다.

### 4. 대기 인스턴스 N+1 배치화

`handleBatch()`를 2-패스로 재구성한다:

- **1패스**: 배치의 모든 메시지를 순회하며 `waitingIndexPort.lookup(tenantId, eventCode, memberId)`로 매칭 후보 `WorkflowInstance` id를 전부 모은다 (Redis 호출은 메시지당 유지, SQL이 아니므로 부담 작음).
- **일괄 조회**: 모아진 id 전체에 대해 `workflowInstanceRepositoryPort.findAllById(ids)` 1회(Spring Data `findAllById` → IN절)로 상태를 가져온다.
- **2패스**: 각 메시지에 대해 1패스에서 만든 맵을 참조해 `workflowEngine.resumeOnMatch()`를 호출한다. 워크플로 정의 조회는 2번 캐시 계층을 사용하므로 여기서 추가 SQL이 발생하지 않는다.

`WorkflowInstanceRepositoryPort`에 `findAllById(ids: Collection<String>): List<WorkflowInstance>` 추가가 필요하다.

### 5. 액션 디스패치 멱등성 가드 (보완)

`dispatchId`가 이미 `(instanceId, nodeId)`로 결정론적으로 생성되지만 소비 시점에 전혀 체크되지 않아, 카프카 재배달이나 타임아웃 폴러의 락 경합 엣지케이스(TTL 만료 후 재획득 등)에서 실제 액션이 중복 실행될 수 있다. `ActionDispatchConsumer`(또는 `DispatchActionUseCase.handle()` 진입부)에서 실행 직전 Redis `SET dispatch-dedup:{dispatchId} 1 NX EX 86400`으로 가드한다 — 실패하면(이미 처리됨) 스킵. 이 설계의 다른 컴포넌트들과 별개로 낮은 비용/높은 효용이라 함께 반영한다.

## 스키마 변경

```sql
-- V2__timeout_poller_tenant_index.sql
CREATE INDEX idx_workflow_instance_tenant_waiting
  ON workflow_instance (tenant_id, status, waiting_until);
```

기존 `idx_workflow_instance_waiting (status, waiting_until)`는 더 이상 폴러 경로에서 쓰이지 않지만, 즉시 제거하지 않고 이번 스코프에서는 유지한다(다른 조회 경로에서 쓰일 가능성 확인 후 별도 정리).

대용량 테이블에 인덱스 추가이므로 운영 반영 시 온라인 DDL(`ALGORITHM=INPLACE` 확인) 또는 저트래픽 시간대 적용을 권장한다.

## 테스트 전략

- **타임아웃 폴러**: 2개 이상 인스턴스를 흉내낸 동시 폴링 시나리오(Testcontainers Redis + MySQL)로 "같은 만료 인스턴스에 대해 합성 이벤트가 정확히 1번만 발행"되는지 검증. 테넌트별 세마포어 상한이 실제로 다른 테넌트 처리량을 침범하지 않는지(한 테넌트 대량 백로그 + 다른 테넌트 소량 백로그 동시 존재 시 후자가 정상 처리되는지) 검증.
- **참조 데이터 캐시**: 캐시 히트 시 포트 호출이 발생하지 않는지, 무효화 이벤트 발행 후 즉시 갱신되는지, TTL 만료 시 재조회되는지 단위 테스트.
- **멤버 쓰기 증폭**: 컨텍스트 미변경 이벤트가 `saveAll` 호출에서 제외되는지, 신규 생성 멤버는 항상 포함되는지 단위 테스트.
- **N+1 배치화**: 대기 인스턴스 여러 건이 섞인 배치에서 `findAllById` 호출이 정확히 1회만 발생하는지(Mock 검증) 단위 테스트.
- **액션 디스패치 멱등성**: 동일 `dispatchId`로 두 번 소비돼도 `actionExecutor.execute()`가 1회만 호출되는지 단위 테스트.

## 열린 리스크 / 후속 과제

- 멀티테넌시 인증/인가/저장소 격리는 이 설계와 완전히 별개의 후속 스펙으로 다룬다.
- Redis 자체가 SPOF가 되는 부분(참조 캐시 무효화, 타임아웃 락, 디스패치 멱등성 가드 모두 Redis 의존) — Redis 장애 시 폴백 전략(예: 캐시는 로컬 TTL로 계속 서비스 가능하나, 락/멱등 가드가 실패하면 fail-open(중복 허용) vs fail-closed(처리 중단) 중 어느 쪽을 택할지)은 별도 논의가 필요하다. 1차 구현은 fail-open(락 실패 시에도 처리 진행, 중복 리스크는 감수)으로 제안하되 확정은 구현 단계에서 재확인한다.
- 타임아웃 폴러 전용 Hikari 풀 크기와 수집 API용 풀 크기의 실제 배분 값은 부하테스트 후 튜닝이 필요하다(이번 스펙은 "분리한다"는 구조적 결정까지만 다룸).
