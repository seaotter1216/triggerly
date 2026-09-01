# triggerly-claude 데모 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `triggerly` 원본과 동일한 헥사고날 구조를 가진 `triggerly-claude`를 만들어, MySQL+ES 이중 저장 회원 관리, DB에 JSON으로 저장되는 워크플로 정의, 이벤트 기반 워크플로 실행 엔진, 이벤트 수집 SDK, 대용량 트래픽 대비(가상 스레드/레이트리밋/Kafka 파티셔닝/부하테스트 도구)까지 실제로 동작하는 데모를 완성한다.

**Architecture:** Kotlin/Spring Boot 헥사고날 아키텍처 7개 Gradle 모듈(domain → application → adapter-{web,persistence,messaging} → bootstrap, 독립 모듈 sdk). 이벤트는 `POST /api/v1/events` → Kafka(`triggerly.events.raw`) → 컨슈머가 MySQL/ES 저장 + `WorkflowEngine` 실행. WAIT_FOR_EVENT는 Redis 역색인으로 매칭, DELAY/타임아웃은 `@Scheduled` 폴러가 감지해 합성 이벤트를 다시 Kafka로 produce.

**Tech Stack:** Kotlin 2.3.21, Spring Boot 4.1.0, Java 21(가상 스레드), Spring Data JPA + MySQL 8, Spring Data Elasticsearch 8.x, Spring for Apache Kafka(KRaft, `apache/kafka:3.7.0`), Spring Data Redis(Lettuce), Flyway, Testcontainers, k6.

**Spec:** `/Users/ben/BenDev/playground/triggerly-claude/docs/superpowers/specs/2026-09-01-triggerly-claude-demo-design.md`

## Global Constraints

- Kotlin `2.3.21` / Spring Boot `4.1.0` / Java 21 툴체인 — 기존 `triggerly/gradle/libs.versions.toml` 값 그대로 사용 (스펙 2절)
- 모듈 의존 방향: `adapter-*, bootstrap → application → domain`. `triggerly-sdk`는 어떤 다른 모듈에도 의존하지 않는다 (스펙 2절)
- Kotlin 패키지 세그먼트에 예약어 `in`/`out`을 쓰지 않는다 — adapter 패키지는 `com.seaotter.triggerly.adapter.web`, `com.seaotter.triggerly.adapter.persistence`로 명명 (Gradle 모듈 이름 `triggerly-adapter-in-web`/`triggerly-adapter-out-persistence`는 원본 그대로 유지, 패키지명만 다름)
- 모든 풀/버퍼/배치/파티션 크기는 `application.yml` 설정값으로 노출한다 — 코드에 하드코딩 금지 (스펙 13절)
- Kafka 파티션 키는 `tenantId:memberId`, 토픽은 `triggerly.events.raw` 단일 토픽 (스펙 5.4, 13.3절)
- `ConditionExpression.Predicate.field`의 `stats.eventCount:<EVENT_CODE>:<N>d` 패턴은 정규식 `^stats\.eventCount:([A-Z0-9_]+):(\d+)d$`로 파싱한다 (스펙 4절)
- 실제 쿠폰/푸시/알림톡 연동, 인증/인가, 관리자 프론트엔드 UI는 만들지 않는다 (스펙 14절 범위 제외)

## File Structure

```
triggerly-claude/
├── settings.gradle.kts
├── build.gradle.kts
├── gradle/libs.versions.toml
├── gradlew, gradlew.bat, gradle/wrapper/*
├── docker-compose.yml
├── docker-compose.observability.yml
├── scripts/demo.sh
├── loadtest/ingest.js, loadtest/README.md
├── README.md
├── triggerly-domain/
│   └── src/main/kotlin/com/seaotter/triggerly/domain/
│       ├── Event.kt, Member.kt, Workflow.kt, ConditionEvaluator.kt
├── triggerly-application/
│   └── src/main/kotlin/com/seaotter/triggerly/application/
│       ├── port/*.kt (포트 인터페이스 전부)
│       ├── engine/WorkflowEngine.kt, engine/ActionExecutor.kt
│       └── usecase/*.kt
├── triggerly-adapter-out-persistence/
│   └── src/main/kotlin/com/seaotter/triggerly/adapter/persistence/
│       ├── json/*.kt (Jackson JPA 컨버터)
│       ├── jpa/*.kt (엔티티 + Spring Data JPA 리포지토리 + 어댑터)
│       └── es/*.kt (ES 문서 + 리포지토리 + 어댑터)
├── triggerly-adapter-messaging/
│   └── src/main/kotlin/com/seaotter/triggerly/adapter/messaging/
│       ├── kafka/*.kt
│       └── redis/*.kt
├── triggerly-adapter-in-web/
│   └── src/main/kotlin/com/seaotter/triggerly/adapter/web/
│       ├── controller/*.kt
│       └── dto/*.kt
├── triggerly-bootstrap/
│   └── src/main/
│       ├── kotlin/com/seaotter/triggerly/bootstrap/*.kt
│       └── resources/{application.yml, application-local.yml, application-demo.yml, db/migration/V1__init.sql}
└── triggerly-sdk/
    └── src/main/kotlin/com/seaotter/triggerly/sdk/TriggerlyClient.kt
```

## Task Index

1. 프로젝트 스캐폴딩 (Gradle 멀티모듈)
2. 도메인 모델 승격 (Event/Member/Workflow + Jackson 다형성 매핑)
3. ConditionEvaluator
4. 애플리케이션 포트 정의
5. WorkflowEngine
6. 유스케이스
7. Flyway 마이그레이션 + JSON 컨버터 + EventDefinition/AttributeDefinition MySQL 어댑터
8. Member MySQL 어댑터
9. EventInstance MySQL 스토어
10. Workflow/WorkflowInstance/WorkflowExecution MySQL 어댑터
11. Elasticsearch 설정 + Member ES 어댑터 + 비동기 동기화
12. EventInstance ES 색인 + MemberEventStatsPort
13. Kafka 프로듀서
14. Redis 대기 인덱스
15. Redis 테넌트 레이트리밋
16. Kafka 컨슈머 (WorkflowTriggerConsumer)
17. 이벤트 수집 REST API
18. 어드민 EventDefinition/AttributeDefinition REST API
19. 어드민 Workflow REST API
20. 어드민 Member REST API + OpenAPI
21. 부트스트랩 (메인/설정/스케줄러/Actuator)
22. DemoRunner + docker-compose
23. triggerly-sdk
24. 통합 테스트 (Testcontainers 풀스택)
25. 부하테스트 도구 (k6)
26. 데모 스크립트 + README

---

### Task 1: 프로젝트 스캐폴딩 (Gradle 멀티모듈)

**Files:**
- Create: `settings.gradle.kts`
- Create: `build.gradle.kts`
- Create: `gradle/libs.versions.toml`
- Copy: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties`, `.java-version` (원본 `triggerly`에서 그대로 복사)
- Create: `triggerly-domain/build.gradle.kts`
- Create: `triggerly-application/build.gradle.kts`
- Create: `triggerly-adapter-out-persistence/build.gradle.kts`
- Create: `triggerly-adapter-messaging/build.gradle.kts`
- Create: `triggerly-adapter-in-web/build.gradle.kts`
- Create: `triggerly-bootstrap/build.gradle.kts`
- Create: `triggerly-sdk/build.gradle.kts`

**Interfaces:**
- Produces: 컴파일 가능한 7개 빈 모듈. 이후 모든 태스크가 이 모듈 디렉터리 안에 파일을 추가한다.

- [ ] **Step 1: 원본 프로젝트에서 Gradle wrapper 복사**

```bash
mkdir -p /Users/ben/BenDev/playground/triggerly-claude
cd /Users/ben/BenDev/playground/triggerly-claude
cp -R /Users/ben/BenDev/playground/triggerly/gradle .
cp /Users/ben/BenDev/playground/triggerly/gradlew .
cp /Users/ben/BenDev/playground/triggerly/gradlew.bat .
cp /Users/ben/BenDev/playground/triggerly/.java-version .
chmod +x gradlew
```

- [ ] **Step 2: `gradle/libs.versions.toml` 작성 (기존 값 + 신규 라이브러리 추가)**

```toml
[versions]
kotlin = "2.3.21"
springBoot = "4.1.0"
archunit = "1.4.1"
mockk = "1.14.7"
kotest = "6.0.4"
springdoc = "2.8.5"

[libraries]
spring-boot-dependencies = { module = "org.springframework.boot:spring-boot-dependencies", version.ref = "springBoot" }
kotlin-reflect = { module = "org.jetbrains.kotlin:kotlin-reflect" }

archunit-junit5 = { module = "com.tngtech.archunit:archunit-junit5", version.ref = "archunit" }

mockk = { module = "io.mockk:mockk", version.ref = "mockk" }
kotest-assertions-core = { module = "io.kotest:kotest-assertions-core", version.ref = "kotest" }
kotlin-test-junit5 = { module = "org.jetbrains.kotlin:kotlin-test-junit5" }
junit-platform-launcher = { module = "org.junit.platform:junit-platform-launcher" }

jackson-module-kotlin = { module = "com.fasterxml.jackson.module:jackson-module-kotlin" }
jackson-datatype-jsr310 = { module = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310" }

spring-boot-starter-web = { module = "org.springframework.boot:spring-boot-starter-web" }
spring-boot-starter-data-jpa = { module = "org.springframework.boot:spring-boot-starter-data-jpa" }
spring-boot-starter-data-elasticsearch = { module = "org.springframework.boot:spring-boot-starter-data-elasticsearch" }
spring-boot-starter-data-redis = { module = "org.springframework.boot:spring-boot-starter-data-redis" }
spring-kafka = { module = "org.springframework.kafka:spring-kafka" }
spring-boot-starter-actuator = { module = "org.springframework.boot:spring-boot-starter-actuator" }
micrometer-registry-prometheus = { module = "io.micrometer:micrometer-registry-prometheus" }
flyway-core = { module = "org.flywaydb:flyway-core" }
flyway-mysql = { module = "org.flywaydb:flyway-mysql" }
mysql-connector-j = { module = "com.mysql:mysql-connector-j" }
springdoc-openapi-webmvc-ui = { module = "org.springdoc:springdoc-openapi-starter-webmvc-ui", version.ref = "springdoc" }

spring-boot-starter-test = { module = "org.springframework.boot:spring-boot-starter-test" }
spring-boot-testcontainers = { module = "org.springframework.boot:spring-boot-testcontainers" }
spring-kafka-test = { module = "org.springframework.kafka:spring-kafka-test" }
testcontainers-junit-jupiter = { module = "org.testcontainers:junit-jupiter" }
testcontainers-mysql = { module = "org.testcontainers:mysql" }
testcontainers-elasticsearch = { module = "org.testcontainers:elasticsearch" }
testcontainers-kafka = { module = "org.testcontainers:kafka" }

[plugins]
kotlin-jvm = { id = "org.jetbrains.kotlin.jvm", version.ref = "kotlin" }
kotlin-spring = { id = "org.jetbrains.kotlin.plugin.spring", version.ref = "kotlin" }
kotlin-jpa = { id = "org.jetbrains.kotlin.plugin.jpa", version.ref = "kotlin" }
spring-boot = { id = "org.springframework.boot", version.ref = "springBoot" }
```

- [ ] **Step 3: `settings.gradle.kts` 작성**

```kotlin
dependencyResolutionManagement {
  @Suppress("UnstableApiUsage")
  repositories {
    mavenCentral()
  }
}

rootProject.name = "triggerly-claude"

include(":triggerly-domain")
include(":triggerly-application")
include(":triggerly-adapter-in-web")
include(":triggerly-adapter-out-persistence")
include(":triggerly-adapter-messaging")
include(":triggerly-bootstrap")
include(":triggerly-sdk")
```

- [ ] **Step 4: 루트 `build.gradle.kts` 작성 (원본과 동일한 패턴 유지)**

```kotlin
plugins {
	alias(libs.plugins.kotlin.jvm) apply false
	alias(libs.plugins.kotlin.spring) apply false
	alias(libs.plugins.spring.boot) apply false
	alias(libs.plugins.kotlin.jpa) apply false
}

val kotlinJvmPluginId = libs.plugins.kotlin.jvm.get().pluginId

val springBootBom = libs.spring.boot.dependencies
val kotlinReflect = libs.kotlin.reflect
val kotlinTestJunit5 = libs.kotlin.test.junit5
val junitPlatformLauncher = libs.junit.platform.launcher

allprojects {
	group = "com.seaotter"
	version = "0.0.1-SNAPSHOT"
	description = "triggerly-claude"
}

subprojects {
	apply(plugin = kotlinJvmPluginId)
	apply(plugin = "java-library")

	extensions.configure<JavaPluginExtension> {
		toolchain {
			languageVersion = JavaLanguageVersion.of(21)
		}
	}

	extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
		compilerOptions {
			freeCompilerArgs.addAll(
				"-Xjsr305=strict",
				"-Xannotation-default-target=param-property",
			)
		}
	}

	dependencies {
		add("implementation", platform(springBootBom))
		add("testImplementation", platform(springBootBom))

		add("implementation", kotlinReflect)

		add("testImplementation", kotlinTestJunit5)
		add("testRuntimeOnly", junitPlatformLauncher)
	}

	tasks.withType<Test>().configureEach {
		useJUnitPlatform()
	}
}
```

- [ ] **Step 5: `triggerly-domain/build.gradle.kts`** (순수 모듈, 추가 의존성 없음 — 루트에서 이미 kotlin-reflect/kotlin-test 부여됨)

```kotlin
dependencies {
}
```

- [ ] **Step 6: `triggerly-application/build.gradle.kts`**

```kotlin
plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation("org.springframework:spring-context")
	implementation("org.springframework:spring-tx")
	testImplementation(libs.mockk)
	testImplementation(libs.kotest.assertions.core)
}
```

- [ ] **Step 7: `triggerly-adapter-out-persistence/build.gradle.kts`**

```kotlin
plugins {
	alias(libs.plugins.kotlin.spring)
	alias(libs.plugins.kotlin.jpa)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.boot.starter.data.elasticsearch)
	implementation(libs.jackson.module.kotlin)
	implementation(libs.jackson.datatype.jsr310)
	runtimeOnly(libs.mysql.connector.j)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.mysql)
	testImplementation(libs.testcontainers.elasticsearch)
	testImplementation(libs.mockk)
}
```

- [ ] **Step 8: `triggerly-adapter-messaging/build.gradle.kts`**

```kotlin
plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(libs.spring.kafka)
	implementation(libs.spring.boot.starter.data.redis)
	implementation(libs.jackson.module.kotlin)
	implementation(libs.jackson.datatype.jsr310)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.kafka)
	testImplementation(libs.spring.kafka.test)
	testImplementation(libs.mockk)
}
```

- [ ] **Step 9: `triggerly-adapter-in-web/build.gradle.kts`**

```kotlin
plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(libs.spring.boot.starter.web)
	implementation(libs.springdoc.openapi.webmvc.ui)
	implementation(libs.jackson.module.kotlin)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.mockk)
}
```

- [ ] **Step 10: `triggerly-bootstrap/build.gradle.kts`**

```kotlin
plugins {
	alias(libs.plugins.kotlin.spring)
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(project(":triggerly-adapter-in-web"))
	implementation(project(":triggerly-adapter-out-persistence"))
	implementation(project(":triggerly-adapter-messaging"))
	implementation(project(":triggerly-sdk"))

	implementation(libs.spring.boot.starter.actuator)
	implementation(libs.micrometer.registry.prometheus)
	implementation(libs.flyway.core)
	implementation(libs.flyway.mysql)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.mysql)
	testImplementation(libs.testcontainers.elasticsearch)
	testImplementation(libs.testcontainers.kafka)
	testImplementation(libs.spring.kafka.test)
	testImplementation(libs.mockk)
}

tasks.named<Jar>("jar") {
	enabled = false
}
```

- [ ] **Step 11: `triggerly-sdk/build.gradle.kts`** (독립 모듈, domain/application 의존 없음)

```kotlin
dependencies {
	implementation(libs.jackson.module.kotlin)
	testImplementation(libs.kotest.assertions.core)
}
```

- [ ] **Step 12: 빌드 확인**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL` (7개 모듈 모두 빈 소스로 컴파일/테스트 통과)

- [ ] **Step 13: Commit**

```bash
git init
git add -A
git commit -m "chore: scaffold triggerly-claude multi-module gradle project"
```

---

### Task 2: 도메인 모델 승격 (Event/Member/Workflow + Jackson 다형성 매핑)

**Files:**
- Create: `triggerly-domain/src/main/kotlin/com/seaotter/triggerly/domain/Event.kt`
- Create: `triggerly-domain/src/main/kotlin/com/seaotter/triggerly/domain/Member.kt`
- Create: `triggerly-domain/src/main/kotlin/com/seaotter/triggerly/domain/Workflow.kt`
- Test: `triggerly-domain/src/test/kotlin/com/seaotter/triggerly/domain/WorkflowJsonRoundTripTest.kt`
- Test: `triggerly-domain/src/test/kotlin/com/seaotter/triggerly/domain/BirthdayParsingTest.kt`
- Delete: 원본을 복사해오지 않는다 — `triggerly-claude`는 새 프로젝트이므로 `triggerly/triggerly-domain/src/test/kotlin`의 세 파일 내용을 참고만 하고, 아래 코드를 `triggerly-claude`에 새로 작성한다.

**Interfaces:**
- Produces: `EventDefinition`, `EventRequest`, `EventInstance`, `AttributeDefinition`, `AttributeType`, `Member`, `MemberContext`, `MemberStatus`, `Gender`, `DevicePlatform`, `Workflow`, `WorkflowInstance`, `WorkflowInstanceStatus`, `WorkflowExecution`, `WorkflowExecutionStatus`, `WorkflowDefinition`, `Node`(+ 6개 하위 타입), `Edge`, `EdgeRoute`(+ 5개 하위 타입), `ConditionExpression`(+ 4개 하위 타입), `ConditionOperator`, `ActionDefinition`(+ 3개 하위 타입), `WaitEventDefinition`, `DurationDto` — 이후 모든 태스크가 이 타입들을 그대로 사용한다.
- **원본 스케치 대비 변경점 (모두 의도적)**:
  1. `WorkflowExecution.status` 타입을 원본의 `WorkflowStatus`(오타)에서 `WorkflowExecutionStatus`로 수정
  2. `WorkflowExecution.status`를 `val`에서 `var`로 변경 — 엔진이 노드 진입 시 RUNNING으로 만들고 완료/재개 시 COMPLETED로 갱신해야 함 (WAIT_FOR_EVENT/DELAY는 대기 중엔 RUNNING으로 남아있다가 재개 시 COMPLETED로 바뀜)
  3. `Node`, `ConditionExpression`, `ActionDefinition`, `EdgeRoute` sealed interface에 Jackson `@JsonTypeInfo`/`@JsonSubTypes`를 붙여 `WorkflowDefinition`이 DB의 JSON 컬럼과 왕복 직렬화되도록 함 (원본엔 없던 요구사항 — DB 저장이 이번 데모의 핵심이므로 필수)

- [ ] **Step 1: `Event.kt` 작성**

```kotlin
package com.seaotter.triggerly.domain

import java.time.LocalDateTime

// 어드민이 등록하는 "이 코드의 이벤트를 받겠다"는 트리거 정보
class EventDefinition(
  val tenantId: String,
  val code: String,
  var displayName: String,
  val createdAt: LocalDateTime = LocalDateTime.now(),
  val id: String = tenantId + code,
)

// SDK/API에서 보내는 요청
class EventRequest(
  val tenantId: String,
  val eventCode: String,
  val member: MemberContext? = null,
  val attributes: Map<String, Any?>? = null,
)

// MySQL엔 30일만 유지, ES엔 로그성으로 저장
class EventInstance(
  val id: String,
  val tenantId: String,
  val eventCode: String,
  val occurredAt: LocalDateTime,
  val memberId: String? = null,
  val attributes: Map<String, Any?>? = null,
)

class AttributeDefinition(
  val id: String,
  val tenantId: String,
  val eventDefinitionId: String? = null,
  val key: String,
  var displayName: String,
  var type: AttributeType,
  var filterable: Boolean = false,
  val createdAt: LocalDateTime = LocalDateTime.now(),
)

enum class AttributeType {
  STRING, LONG, DOUBLE, BIG_DECIMAL, BOOLEAN, DATE
}
```

- [ ] **Step 2: `Member.kt` 작성**

```kotlin
package com.seaotter.triggerly.domain

import java.time.LocalDate
import java.time.LocalDateTime

class Member(
  val id: String,
  val tenantId: String,
  val externalMemberId: String,
  val name: String? = null,
  var email: String? = null,
  var telephone: String? = null,
  var devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  var birthday: LocalDate? = null,

  var status: MemberStatus? = null,

  val joinedAt: LocalDateTime? = null,
  var lastLoginAt: LocalDateTime? = null,
  var withdrawnAt: LocalDateTime? = null,
  val createdAt: LocalDateTime = LocalDateTime.now(),

  var marketingSmsAgreed: Boolean = false,
  var marketingPushAgreed: Boolean = false,
  var marketingEmailAgreed: Boolean = false,
  var marketingKakaoAgreed: Boolean = false,
  var marketingAgreedAt: LocalDateTime? = null,

  var attributes: Map<String, Any?>? = null,
)

enum class MemberStatus { ACTIVE, WITHDRAWN, DORMANT, BLOCKED }
enum class Gender { MALE, FEMALE }
enum class DevicePlatform { ANDROID, IOS, WEB }

// EventRequest에 실려오는 회원 컨텍스트 (SDK 호출부가 채워서 보냄)
class MemberContext(
  val tenantId: String,
  val externalMemberId: String,
  val name: String? = null,
  var email: String? = null,
  var telephone: String? = null,
  var devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  var birthday: LocalDate? = null,
  var status: MemberStatus? = null,
  val joinedAt: LocalDateTime? = null,
  var lastLoginAt: LocalDateTime? = null,
  var withdrawnAt: LocalDateTime? = null,
  val createdAt: LocalDateTime = LocalDateTime.now(),
  var marketingSmsAgreed: Boolean = false,
  var marketingPushAgreed: Boolean = false,
  var marketingEmailAgreed: Boolean = false,
  var marketingKakaoAgreed: Boolean = false,
  var marketingAgreedAt: LocalDateTime? = null,
)
```

- [ ] **Step 3: `Workflow.kt` 작성 (Jackson 다형성 매핑 포함)**

```kotlin
package com.seaotter.triggerly.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit

class Workflow(
  val id: String,
  val tenantId: String,
  val name: String? = null,
  val triggerEventCode: String,
  val definitionJson: WorkflowDefinition,
  var version: Long = 1,
  var status: WorkflowStatus,
  val createdAt: LocalDateTime,
  var lastUpdatedAt: LocalDateTime,
)

enum class WorkflowStatus { DRAFT, ENABLED, DISABLED, ARCHIVED }

class WorkflowInstance(
  val id: String,
  val workflowId: String,
  val tenantId: String,
  val triggerEventCode: String,
  val version: Long,
  val memberId: String? = null,
  var status: WorkflowInstanceStatus,
  var currentNodeId: String? = null,
  var waitingEventName: String? = null,
  var waitingUntil: LocalDateTime? = null,
  var startedAt: LocalDateTime? = null,
  var completedAt: LocalDateTime? = null,
)

enum class WorkflowInstanceStatus { RUNNING, WAITING, COMPLETED, EXPIRED, ERROR }

class WorkflowExecution(
  val id: String,
  val workflowInstanceId: String,
  val nodeId: String,
  val nodeType: NodeType,
  var status: WorkflowExecutionStatus,
  var startedAt: LocalDateTime? = null,
  var completedAt: LocalDateTime? = null,
  var result: String? = null,
  var errorCode: String? = null,
)

enum class WorkflowExecutionStatus { RUNNING, COMPLETED, FAILED, EXPIRED }

class WorkflowDefinition(
  val trigger: String,
  val nodes: List<Node>,
  val edges: List<Edge>,
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  JsonSubTypes.Type(value = Node.Trigger::class, name = "TRIGGER"),
  JsonSubTypes.Type(value = Node.Condition::class, name = "CONDITION"),
  JsonSubTypes.Type(value = Node.Action::class, name = "ACTION"),
  JsonSubTypes.Type(value = Node.WaitForEvent::class, name = "WAIT_FOR_EVENT"),
  JsonSubTypes.Type(value = Node.Delay::class, name = "DELAY"),
  JsonSubTypes.Type(value = Node.End::class, name = "END"),
)
sealed interface Node {
  val id: String

  data class Trigger(override val id: String, val eventCode: String) : Node
  data class Condition(override val id: String, val condition: ConditionExpression) : Node
  data class Action(override val id: String, val action: ActionDefinition) : Node
  data class WaitForEvent(override val id: String, val event: WaitEventDefinition) : Node
  data class Delay(override val id: String, val duration: DurationDto) : Node
  data class End(override val id: String) : Node
}

data class DurationDto(val value: Long, val unit: DurationUnit) {
  fun toDuration(): Duration = when (unit) {
    DurationUnit.SECONDS -> value.seconds
    DurationUnit.MINUTES -> value.minutes
    DurationUnit.HOURS -> value.hours
    DurationUnit.DAYS -> value.days
    else -> error("unsupported DurationUnit: $unit")
  }
}

enum class NodeType { TRIGGER, CONDITION, ACTION, WAIT_FOR_EVENT, DELAY, END }

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  JsonSubTypes.Type(value = ConditionExpression.Predicate::class, name = "PREDICATE"),
  JsonSubTypes.Type(value = ConditionExpression.And::class, name = "AND"),
  JsonSubTypes.Type(value = ConditionExpression.Or::class, name = "OR"),
  JsonSubTypes.Type(value = ConditionExpression.Not::class, name = "NOT"),
)
sealed interface ConditionExpression {
  data class Predicate(val field: String, val operator: ConditionOperator, val value: Any?) : ConditionExpression
  data class And(val conditions: List<ConditionExpression>) : ConditionExpression
  data class Or(val conditions: List<ConditionExpression>) : ConditionExpression
  data class Not(val condition: ConditionExpression) : ConditionExpression
}

enum class ConditionOperator { EQ, NE, GT, GOE, LT, LOE, IN, NOT_IN, EXISTS }

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  JsonSubTypes.Type(value = ActionDefinition.IssueCoupon::class, name = "ISSUE_COUPON"),
  JsonSubTypes.Type(value = ActionDefinition.SendPush::class, name = "SEND_PUSH"),
  JsonSubTypes.Type(value = ActionDefinition.SendAlimTalk::class, name = "SEND_ALIM_TALK"),
)
sealed interface ActionDefinition {
  data class IssueCoupon(val couponId: String) : ActionDefinition
  data class SendPush(val templateId: String) : ActionDefinition
  data class SendAlimTalk(val templateId: String) : ActionDefinition
}

data class WaitEventDefinition(
  val eventCode: String,
  val timeout: DurationDto? = null,
  val matchConditions: ConditionExpression? = null,
)

data class Edge(val from: String, val to: String, val route: EdgeRoute)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  JsonSubTypes.Type(value = EdgeRoute.Always::class, name = "ALWAYS"),
  JsonSubTypes.Type(value = EdgeRoute.True::class, name = "TRUE"),
  JsonSubTypes.Type(value = EdgeRoute.False::class, name = "FALSE"),
  JsonSubTypes.Type(value = EdgeRoute.Matched::class, name = "MATCHED"),
  JsonSubTypes.Type(value = EdgeRoute.Timeout::class, name = "TIMEOUT"),
)
sealed interface EdgeRoute {
  data object Always : EdgeRoute
  data object True : EdgeRoute
  data object False : EdgeRoute
  data object Matched : EdgeRoute
  data object Timeout : EdgeRoute
}
```

- [ ] **Step 4: JSON 왕복 테스트 작성 (실패 확인 전 — Jackson 모듈이 domain에 없으므로 이 테스트는 domain 모듈에 jackson-module-kotlin을 테스트 의존성으로 추가해야 함)**

`triggerly-domain/build.gradle.kts`에 다음 한 줄 추가:

```kotlin
dependencies {
	testImplementation(libs.jackson.module.kotlin)
}
```

`triggerly-domain/src/test/kotlin/com/seaotter/triggerly/domain/WorkflowJsonRoundTripTest.kt`:

```kotlin
package com.seaotter.triggerly.domain

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.DurationUnit

class WorkflowJsonRoundTripTest {

  private val mapper: ObjectMapper = jacksonObjectMapper()

  @Test
  fun `WorkflowDefinition은 모든 노드/엣지 타입을 포함해 JSON으로 직렬화 후 복원할 수 있다`() {
    val definition = WorkflowDefinition(
      trigger = "PURCHASE",
      nodes = listOf(
        Node.Trigger(id = "n1", eventCode = "PURCHASE"),
        Node.WaitForEvent(
          id = "n2",
          event = WaitEventDefinition(
            eventCode = "REVIEW_ADD",
            timeout = DurationDto(5, DurationUnit.MINUTES),
          ),
        ),
        Node.Condition(
          id = "n4",
          condition = ConditionExpression.Predicate(
            field = "stats.eventCount:REVIEW_ADD:30d",
            operator = ConditionOperator.GOE,
            value = 3,
          ),
        ),
        Node.Action(id = "n5", action = ActionDefinition.SendAlimTalk(templateId = "review-nudge")),
        Node.Delay(id = "d1", duration = DurationDto(30, DurationUnit.SECONDS)),
        Node.End(id = "n7"),
      ),
      edges = listOf(
        Edge(from = "n1", to = "n2", route = EdgeRoute.Always),
        Edge(from = "n2", to = "n4", route = EdgeRoute.Timeout),
        Edge(from = "n4", to = "n5", route = EdgeRoute.True),
      ),
    )

    val json = mapper.writeValueAsString(definition)
    val restored: WorkflowDefinition = mapper.readValue(json)

    assertEquals(definition.nodes.size, restored.nodes.size)
    assertEquals(definition.edges.size, restored.edges.size)
    assert(restored.nodes[1] is Node.WaitForEvent)
    assert(restored.edges[1].route is EdgeRoute.Timeout)
    val restoredCondition = (restored.nodes[2] as Node.Condition).condition as ConditionExpression.Predicate
    assertEquals("stats.eventCount:REVIEW_ADD:30d", restoredCondition.field)
  }
}
```

- [ ] **Step 5: 테스트 실행하여 통과 확인 (구현이 이미 Step 3에서 끝났으므로 바로 통과해야 함 — 실패하면 `@JsonSubTypes` 매핑 오타를 의심)**

Run: `./gradlew :triggerly-domain:test --tests "com.seaotter.triggerly.domain.WorkflowJsonRoundTripTest"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 6: 생일 파싱 유틸 테스트 (원본 `BirthdayTest` 승격 — `LocalDate.parse`가 `yyyyMMdd` 포맷을 잘 다루는지 확인하는 순수 회귀 테스트)**

`triggerly-domain/src/test/kotlin/com/seaotter/triggerly/domain/BirthdayParsingTest.kt`:

```kotlin
package com.seaotter.triggerly.domain

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.test.Test
import kotlin.test.assertEquals

class BirthdayParsingTest {

  @Test
  fun `yyyyMMdd 포맷 생일 문자열을 LocalDate로 파싱할 수 있다`() {
    val birthday = "20250305"
    val date = LocalDate.parse(birthday, DateTimeFormatter.ofPattern("yyyyMMdd"))
    assertEquals(LocalDate.of(2025, 3, 5), date)
  }
}
```

- [ ] **Step 7: 전체 domain 테스트 실행**

Run: `./gradlew :triggerly-domain:test`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 8: Commit**

```bash
git add triggerly-domain
git commit -m "feat(domain): promote Event/Member/Workflow models with Jackson polymorphic mapping"
```

---

### Task 3: ConditionEvaluator

**Files:**
- Create: `triggerly-domain/src/main/kotlin/com/seaotter/triggerly/domain/ConditionEvaluator.kt`
- Test: `triggerly-domain/src/test/kotlin/com/seaotter/triggerly/domain/ConditionEvaluatorTest.kt`

**Interfaces:**
- Consumes: `ConditionExpression`, `ConditionOperator` (Task 2)
- Produces: `EventStatsResolver` (fun interface), `ConditionEvaluator.evaluate(expr, context, statsResolver): Boolean` — Task 5(WorkflowEngine)와 Task 6(application의 어댑팅 래퍼)가 그대로 사용한다.

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.domain

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ConditionEvaluatorTest {

  private val noStats = EventStatsResolver { _, _, _ -> error("stats should not be called") }

  @Test
  fun `EQ 연산자는 컨텍스트 값과 일치하면 true`() {
    val expr = ConditionExpression.Predicate("event.amount", ConditionOperator.EQ, 50000)
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("event.amount" to 50000), noStats))
  }

  @Test
  fun `GOE 연산자는 숫자를 Number로 통일해서 비교한다`() {
    val expr = ConditionExpression.Predicate("event.amount", ConditionOperator.GOE, 50000)
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("event.amount" to 50000.0), noStats))
    assertFalse(ConditionEvaluator.evaluate(expr, mapOf("event.amount" to 49999), noStats))
  }

  @Test
  fun `IN 연산자는 value가 List일 때 포함 여부를 확인한다`() {
    val expr = ConditionExpression.Predicate("member.city", ConditionOperator.IN, listOf("서울", "부산"))
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("member.city" to "서울"), noStats))
    assertFalse(ConditionEvaluator.evaluate(expr, mapOf("member.city" to "대전"), noStats))
  }

  @Test
  fun `EXISTS 연산자는 컨텍스트에 키가 있는지만 확인한다`() {
    val expr = ConditionExpression.Predicate("member.email", ConditionOperator.EXISTS, null)
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("member.email" to "a@b.com"), noStats))
    assertFalse(ConditionEvaluator.evaluate(expr, emptyMap(), noStats))
  }

  @Test
  fun `And Or Not 조합을 평가할 수 있다`() {
    val expr = ConditionExpression.And(
      listOf(
        ConditionExpression.Predicate("event.amount", ConditionOperator.GOE, 10000),
        ConditionExpression.Or(
          listOf(
            ConditionExpression.Predicate("member.city", ConditionOperator.EQ, "서울"),
            ConditionExpression.Not(ConditionExpression.Predicate("member.vip", ConditionOperator.EQ, false)),
          ),
        ),
      ),
    )
    val ctx = mapOf("event.amount" to 15000, "member.city" to "대전", "member.vip" to true)
    assertTrue(ConditionEvaluator.evaluate(expr, ctx, noStats))
  }

  @Test
  fun `stats eventCount 필드는 컨텍스트가 아니라 EventStatsResolver를 호출한다`() {
    val expr = ConditionExpression.Predicate("stats.eventCount:REVIEW_ADD:30d", ConditionOperator.GOE, 3)
    val resolver = EventStatsResolver { memberId, eventCode, withinDays ->
      assertTrue(memberId == "m1" && eventCode == "REVIEW_ADD" && withinDays == 30)
      3L
    }
    assertTrue(
      ConditionEvaluator.evaluate(expr, mapOf("member.id" to "m1"), resolver, memberId = "m1"),
    )
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-domain:test --tests "com.seaotter.triggerly.domain.ConditionEvaluatorTest"`
Expected: FAIL (컴파일 에러 — `ConditionEvaluator`, `EventStatsResolver` 없음)

- [ ] **Step 3: `ConditionEvaluator.kt` 구현**

```kotlin
package com.seaotter.triggerly.domain

fun interface EventStatsResolver {
  fun countEvents(memberId: String, eventCode: String, withinDays: Int): Long
}

private val STATS_FIELD_REGEX = Regex("""^stats\.eventCount:([A-Z0-9_]+):(\d+)d$""")

object ConditionEvaluator {

  fun evaluate(
    expr: ConditionExpression,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver,
    memberId: String? = null,
  ): Boolean = when (expr) {
    is ConditionExpression.Predicate -> evaluatePredicate(expr, context, statsResolver, memberId)
    is ConditionExpression.And -> expr.conditions.all { evaluate(it, context, statsResolver, memberId) }
    is ConditionExpression.Or -> expr.conditions.any { evaluate(it, context, statsResolver, memberId) }
    is ConditionExpression.Not -> !evaluate(expr.condition, context, statsResolver, memberId)
  }

  private fun evaluatePredicate(
    predicate: ConditionExpression.Predicate,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver,
    memberId: String?,
  ): Boolean {
    if (predicate.operator == ConditionOperator.EXISTS) {
      return context.containsKey(predicate.field) && context[predicate.field] != null
    }

    val actual = resolveValue(predicate.field, context, statsResolver, memberId)
    return compare(actual, predicate.operator, predicate.value)
  }

  private fun resolveValue(
    field: String,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver,
    memberId: String?,
  ): Any? {
    val statsMatch = STATS_FIELD_REGEX.matchEntire(field)
    if (statsMatch != null) {
      requireNotNull(memberId) { "stats.eventCount 조건을 평가하려면 memberId가 필요합니다: $field" }
      val (eventCode, withinDays) = statsMatch.destructured
      return statsResolver.countEvents(memberId, eventCode, withinDays.toInt())
    }
    return context[field]
  }

  private fun compare(actual: Any?, operator: ConditionOperator, expected: Any?): Boolean = when (operator) {
    ConditionOperator.EQ -> actual == expected
    ConditionOperator.NE -> actual != expected
    ConditionOperator.GT -> compareNumbers(actual, expected) { a, b -> a > b }
    ConditionOperator.GOE -> compareNumbers(actual, expected) { a, b -> a >= b }
    ConditionOperator.LT -> compareNumbers(actual, expected) { a, b -> a < b }
    ConditionOperator.LOE -> compareNumbers(actual, expected) { a, b -> a <= b }
    ConditionOperator.IN -> (expected as? List<*>)?.contains(actual) ?: false
    ConditionOperator.NOT_IN -> (expected as? List<*>)?.contains(actual)?.not() ?: true
    ConditionOperator.EXISTS -> error("EXISTS는 evaluatePredicate에서 먼저 처리됨")
  }

  private fun compareNumbers(actual: Any?, expected: Any?, cmp: (Double, Double) -> Boolean): Boolean {
    val a = (actual as? Number)?.toDouble() ?: return false
    val b = (expected as? Number)?.toDouble() ?: return false
    return cmp(a, b)
  }
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-domain:test --tests "com.seaotter.triggerly.domain.ConditionEvaluatorTest"`
Expected: `BUILD SUCCESSFUL`, 6 tests passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-domain
git commit -m "feat(domain): add ConditionEvaluator with local-context and ES stats field support"
```

---

### Task 4: 애플리케이션 포트 정의

**Files:**
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/EventDefinitionPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/AttributeDefinitionPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/MemberCommandPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/MemberQueryPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/EventInstanceRepositoryPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/MemberEventStatsPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowRepositoryPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowInstanceRepositoryPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WorkflowExecutionRepositoryPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/EventPublisherPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/WaitingIndexPort.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/port/RateLimiterPort.kt`

**Interfaces:**
- Consumes: 도메인 타입 전부 (Task 2)
- Produces: 아래 시그니처 그대로 — Task 5~26 전체가 이 파일들의 인터페이스명/메서드명을 그대로 참조한다. 이 태스크는 순수 인터페이스라 단위 테스트 대상이 없고, 컴파일 통과가 검증 기준이다.

- [ ] **Step 1: `EventDefinitionPort.kt`**

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.EventDefinition

interface EventDefinitionPort {
  fun save(definition: EventDefinition): EventDefinition
  fun findByTenantAndCode(tenantId: String, code: String): EventDefinition?
  fun findAll(tenantId: String): List<EventDefinition>
}
```

- [ ] **Step 2: `AttributeDefinitionPort.kt`**

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.AttributeDefinition

interface AttributeDefinitionPort {
  fun save(definition: AttributeDefinition): AttributeDefinition
  fun findAll(tenantId: String): List<AttributeDefinition>
}
```

- [ ] **Step 3: `MemberCommandPort.kt` / `MemberQueryPort.kt`**

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Member

interface MemberCommandPort {
  fun save(member: Member): Member
  fun findById(id: String): Member?
  fun findByExternalId(tenantId: String, externalMemberId: String): Member?
}
```

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Member

interface MemberQueryPort {
  // criteria key는 "attributes.<key>" 또는 "city"/"status" 같은 최상위 필드명
  fun search(tenantId: String, criteria: Map<String, Any?>, limit: Int = 50): List<Member>
}
```

- [ ] **Step 4: `EventInstanceRepositoryPort.kt` / `MemberEventStatsPort.kt`**

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.EventInstance
import java.time.LocalDateTime

interface EventInstanceRepositoryPort {
  fun save(instance: EventInstance): EventInstance
  fun deleteOlderThan(cutoff: LocalDateTime): Int
}
```

```kotlin
package com.seaotter.triggerly.application.port

interface MemberEventStatsPort {
  fun countEvents(tenantId: String, memberId: String, eventCode: String, withinDays: Int): Long
}
```

- [ ] **Step 5: `WorkflowRepositoryPort.kt` / `WorkflowInstanceRepositoryPort.kt` / `WorkflowExecutionRepositoryPort.kt`**

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Workflow

interface WorkflowRepositoryPort {
  fun save(workflow: Workflow): Workflow
  fun findById(tenantId: String, id: String): Workflow?
  fun findAll(tenantId: String): List<Workflow>
  fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow>
}
```

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.WorkflowInstance
import java.time.LocalDateTime

interface WorkflowInstanceRepositoryPort {
  fun save(instance: WorkflowInstance): WorkflowInstance
  fun findById(id: String): WorkflowInstance?
  fun findWaitingExpired(now: LocalDateTime, limit: Int = 200): List<WorkflowInstance>
}
```

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.WorkflowExecution

interface WorkflowExecutionRepositoryPort {
  fun save(execution: WorkflowExecution): WorkflowExecution
  fun findByInstanceId(instanceId: String): List<WorkflowExecution>
  fun findRunning(instanceId: String, nodeId: String): WorkflowExecution?
}
```

- [ ] **Step 6: `EventPublisherPort.kt` / `WaitingIndexPort.kt` / `RateLimiterPort.kt`**

```kotlin
package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.MemberContext
import java.time.LocalDateTime

// externalMemberId만 담아 파티션 키(tenantId:externalMemberId)로 쓴다 — 컨트롤러가 내부 memberId를
// 조회/생성하려고 DB를 타지 않게 하기 위함 (13.1절, 스레드 고갈 방지). memberContext가 있으면
// 컨슈머가 Member를 upsert하고, 내부 memberId는 EventInstance/WorkflowInstance 저장 시점에만 등장한다.
data class RawEventMessage(
  val tenantId: String,
  val eventCode: String,
  val externalMemberId: String?,
  val memberContext: MemberContext?,
  val attributes: Map<String, Any?>?,
  val occurredAt: LocalDateTime,
  val syntheticTimeoutForInstanceId: String? = null,
)

fun interface EventPublisherPort {
  fun publish(message: RawEventMessage)
}
```

```kotlin
package com.seaotter.triggerly.application.port

interface WaitingIndexPort {
  fun register(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String)
  fun remove(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String)
  fun lookup(tenantId: String, eventCode: String, memberId: String): List<String>
}
```

```kotlin
package com.seaotter.triggerly.application.port

fun interface RateLimiterPort {
  // true = 허용, false = 한도 초과
  fun tryConsume(tenantId: String): Boolean
}
```

- [ ] **Step 7: 컴파일 확인**

Run: `./gradlew :triggerly-application:compileKotlin`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 8: Commit**

```bash
git add triggerly-application
git commit -m "feat(application): define hexagonal ports"
```

---

### Task 5: WorkflowEngine

**Files:**
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/engine/ActionExecutor.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/engine/WorkflowEngine.kt`
- Test: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/engine/WorkflowEngineTest.kt`

**Interfaces:**
- Consumes: `WorkflowInstanceRepositoryPort`, `WorkflowExecutionRepositoryPort`, `WaitingIndexPort`, `MemberEventStatsPort` (Task 4), 도메인 타입 전부(Task 2), `ConditionEvaluator`/`EventStatsResolver`(Task 3)
- Produces:
  - `ActionExecutor.execute(action: ActionDefinition): String`
  - `WorkflowEngine.start(workflow: Workflow, tenantId: String, memberId: String?, context: Map<String, Any?>): WorkflowInstance`
  - `WorkflowEngine.resumeOnMatch(instance: WorkflowInstance, workflow: Workflow, eventContext: Map<String, Any?>): WorkflowInstance`
  - `WorkflowEngine.resumeOnTimeout(instance: WorkflowInstance, workflow: Workflow): WorkflowInstance`
  -이후 Task 6(`IngestEventUseCase`)이 이 세 메서드를 그대로 호출한다.
- **설계 결정**: `member.*` 컨텍스트(이벤트+회원 속성 병합)는 엔진이 아니라 호출자(Task 6 `IngestEventUseCase`)가 만들어서 넘긴다 — 엔진은 Member를 모른다. `resumeOnTimeout`은 트리거 이벤트가 없으므로 빈 컨텍스트(`emptyMap()`)로 이어지는 노드를 실행한다 (데모 시나리오 4의 타임아웃 이후 CONDITION은 `stats.eventCount` 필드만 쓰므로 컨텍스트가 비어도 문제없음).

- [ ] **Step 1: `ActionExecutor.kt` 구현 (데모용 모의 실행 — 실제 쿠폰/푸시/알림톡 연동 없음, 스펙 14절)**

```kotlin
package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.domain.ActionDefinition
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ActionExecutor {
  private val log = LoggerFactory.getLogger(ActionExecutor::class.java)

  fun execute(action: ActionDefinition): String {
    val description = when (action) {
      is ActionDefinition.IssueCoupon -> "쿠폰 발급: couponId=${action.couponId}"
      is ActionDefinition.SendPush -> "푸시 발송: templateId=${action.templateId}"
      is ActionDefinition.SendAlimTalk -> "알림톡 발송: templateId=${action.templateId}"
    }
    log.info("[DEMO ACTION] {}", description)
    return description
  }
}
```

- [ ] **Step 2: 실패하는 테스트 작성 (fake 포트 + 4개 데모 시나리오 조립)**

```kotlin
package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.application.port.MemberEventStatsPort
import com.seaotter.triggerly.application.port.WaitingIndexPort
import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.DurationUnit
import java.time.LocalDateTime

private class FakeWorkflowInstanceRepository : WorkflowInstanceRepositoryPort {
  val store = mutableMapOf<String, WorkflowInstance>()
  override fun save(instance: WorkflowInstance): WorkflowInstance { store[instance.id] = instance; return instance }
  override fun findById(id: String): WorkflowInstance? = store[id]
  override fun findWaitingExpired(now: LocalDateTime, limit: Int) =
    store.values.filter { it.status == WorkflowInstanceStatus.WAITING && it.waitingUntil?.isAfter(now) == false }
}

private class FakeWorkflowExecutionRepository : WorkflowExecutionRepositoryPort {
  val store = mutableMapOf<String, WorkflowExecution>()
  override fun save(execution: WorkflowExecution): WorkflowExecution { store[execution.id] = execution; return execution }
  override fun findByInstanceId(instanceId: String) = store.values.filter { it.workflowInstanceId == instanceId }
  override fun findRunning(instanceId: String, nodeId: String) =
    store.values.firstOrNull { it.workflowInstanceId == instanceId && it.nodeId == nodeId && it.status == WorkflowExecutionStatus.RUNNING }
}

private class FakeWaitingIndexPort : WaitingIndexPort {
  val index = mutableMapOf<String, MutableList<String>>()
  private fun key(t: String, e: String, m: String) = "$t:$e:$m"
  override fun register(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String) {
    index.getOrPut(key(tenantId, eventCode, memberId)) { mutableListOf() }.add(workflowInstanceId)
  }
  override fun remove(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String) {
    index[key(tenantId, eventCode, memberId)]?.remove(workflowInstanceId)
  }
  override fun lookup(tenantId: String, eventCode: String, memberId: String) =
    index[key(tenantId, eventCode, memberId)]?.toList() ?: emptyList()
}

private class FakeMemberEventStatsPort(private val counts: Map<String, Long>) : MemberEventStatsPort {
  override fun countEvents(tenantId: String, memberId: String, eventCode: String, withinDays: Int) =
    counts["$memberId:$eventCode"] ?: 0L
}

class WorkflowEngineTest {

  private fun workflow(definition: WorkflowDefinition, triggerEventCode: String = "TRIGGER") = Workflow(
    id = "wf-1",
    tenantId = "tenant-1",
    triggerEventCode = triggerEventCode,
    definitionJson = definition,
    status = WorkflowStatus.ENABLED,
    createdAt = LocalDateTime.now(),
    lastUpdatedAt = LocalDateTime.now(),
  )

  private fun engine(counts: Map<String, Long> = emptyMap()) = Triple(
    WorkflowEngine(
      FakeWorkflowInstanceRepository(),
      FakeWorkflowExecutionRepository(),
      FakeWaitingIndexPort(),
      FakeMemberEventStatsPort(counts),
      ActionExecutor(),
    ),
    FakeWaitingIndexPort(),
    counts,
  )

  @Test
  fun `시나리오 생일쿠폰 - Condition True 분기로 Action을 거쳐 End까지 진행한다`() {
    val definition = WorkflowDefinition(
      trigger = "LOGIN",
      nodes = listOf(
        Node.Trigger("n1", "LOGIN"),
        Node.Condition("n2", ConditionExpression.Predicate("member.isBirthdayToday", ConditionOperator.EQ, true)),
        Node.Action("n3", ActionDefinition.IssueCoupon("BIRTHDAY10")),
        Node.End("n4"),
        Node.End("n5"),
      ),
      edges = listOf(
        Edge("n1", "n2", EdgeRoute.Always),
        Edge("n2", "n3", EdgeRoute.True),
        Edge("n2", "n5", EdgeRoute.False),
        Edge("n3", "n4", EdgeRoute.Always),
      ),
    )
    val (engine, _, _) = engine()
    val instance = engine.start(
      workflow(definition, "LOGIN"),
      tenantId = "tenant-1",
      memberId = "m1",
      context = mapOf("member.isBirthdayToday" to true),
    )
    assertEquals(WorkflowInstanceStatus.COMPLETED, instance.status)
    assertEquals("n4", instance.currentNodeId)
  }

  @Test
  fun `WaitForEvent 노드는 인스턴스를 WAITING으로 만들고 대기 인덱스에 등록한다`() {
    val definition = WorkflowDefinition(
      trigger = "CART_ADD",
      nodes = listOf(
        Node.Trigger("n1", "CART_ADD"),
        Node.WaitForEvent("n2", WaitEventDefinition("PURCHASE", DurationDto(1, DurationUnit.MINUTES))),
        Node.End("n3"),
        Node.Action("n4", ActionDefinition.IssueCoupon("REMIND10")),
        Node.End("n5"),
      ),
      edges = listOf(
        Edge("n1", "n2", EdgeRoute.Always),
        Edge("n2", "n3", EdgeRoute.Matched),
        Edge("n2", "n4", EdgeRoute.Timeout),
        Edge("n4", "n5", EdgeRoute.Always),
      ),
    )
    val waitingIndex = FakeWaitingIndexPort()
    val engine = WorkflowEngine(
      FakeWorkflowInstanceRepository(),
      FakeWorkflowExecutionRepository(),
      waitingIndex,
      FakeMemberEventStatsPort(emptyMap()),
      ActionExecutor(),
    )
    val instance = engine.start(workflow(definition, "CART_ADD"), "tenant-1", "m1", emptyMap())
    assertEquals(WorkflowInstanceStatus.WAITING, instance.status)
    assertEquals("n2", instance.currentNodeId)
    assertEquals(listOf(instance.id), waitingIndex.lookup("tenant-1", "PURCHASE", "m1"))

    val resumed = engine.resumeOnMatch(instance, workflow(definition, "CART_ADD"), emptyMap())
    assertEquals(WorkflowInstanceStatus.COMPLETED, resumed.status)
    assertEquals("n3", resumed.currentNodeId)
    assertTrue(waitingIndex.lookup("tenant-1", "PURCHASE", "m1").isEmpty())
  }

  @Test
  fun `시나리오 리뷰유도 - Timeout 이후 ES 집계 Condition이 True면 알림톡 액션으로 이어진다`() {
    val definition = WorkflowDefinition(
      trigger = "PURCHASE",
      nodes = listOf(
        Node.Trigger("n1", "PURCHASE"),
        Node.WaitForEvent("n2", WaitEventDefinition("REVIEW_ADD", DurationDto(5, DurationUnit.MINUTES))),
        Node.Action("n3", ActionDefinition.IssueCoupon("REVIEW10")),
        Node.End("n6"),
        Node.Condition("n4", ConditionExpression.Predicate("stats.eventCount:REVIEW_ADD:30d", ConditionOperator.GOE, 3)),
        Node.Action("n5", ActionDefinition.SendAlimTalk("review-nudge")),
        Node.End("n8"),
        Node.End("n7"),
      ),
      edges = listOf(
        Edge("n1", "n2", EdgeRoute.Always),
        Edge("n2", "n3", EdgeRoute.Matched),
        Edge("n3", "n6", EdgeRoute.Always),
        Edge("n2", "n4", EdgeRoute.Timeout),
        Edge("n4", "n5", EdgeRoute.True),
        Edge("n4", "n7", EdgeRoute.False),
        Edge("n5", "n8", EdgeRoute.Always),
      ),
    )
    val engine = WorkflowEngine(
      FakeWorkflowInstanceRepository(),
      FakeWorkflowExecutionRepository(),
      FakeWaitingIndexPort(),
      FakeMemberEventStatsPort(mapOf("m1:REVIEW_ADD" to 3L)),
      ActionExecutor(),
    )
    val wf = workflow(definition, "PURCHASE")
    val instance = engine.start(wf, "tenant-1", "m1", emptyMap())
    assertEquals(WorkflowInstanceStatus.WAITING, instance.status)

    val resumed = engine.resumeOnTimeout(instance, wf)
    assertEquals(WorkflowInstanceStatus.COMPLETED, resumed.status)
    assertEquals("n8", resumed.currentNodeId)
  }

  @Test
  fun `Delay 노드는 WAITING 상태로 대기했다가 타임아웃 시 Always 엣지로 진행한다`() {
    val definition = WorkflowDefinition(
      trigger = "SIGN_UP",
      nodes = listOf(
        Node.Trigger("n1", "SIGN_UP"),
        Node.Delay("n2", DurationDto(30, DurationUnit.SECONDS)),
        Node.Action("n3", ActionDefinition.SendAlimTalk("welcome")),
        Node.End("n4"),
      ),
      edges = listOf(
        Edge("n1", "n2", EdgeRoute.Always),
        Edge("n2", "n3", EdgeRoute.Always),
        Edge("n3", "n4", EdgeRoute.Always),
      ),
    )
    val (engine, _, _) = engine()
    val wf = workflow(definition, "SIGN_UP")
    val instance = engine.start(wf, "tenant-1", "m1", emptyMap())
    assertEquals(WorkflowInstanceStatus.WAITING, instance.status)
    assertNull(instance.waitingEventName)

    val resumed = engine.resumeOnTimeout(instance, wf)
    assertEquals(WorkflowInstanceStatus.COMPLETED, resumed.status)
    assertEquals("n4", resumed.currentNodeId)
  }
}
```

- [ ] **Step 3: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "com.seaotter.triggerly.application.engine.WorkflowEngineTest"`
Expected: FAIL (컴파일 에러 — `WorkflowEngine` 없음)

- [ ] **Step 4: `WorkflowEngine.kt` 구현**

```kotlin
package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.application.port.MemberEventStatsPort
import com.seaotter.triggerly.application.port.WaitingIndexPort
import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.*
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class WorkflowEngine(
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val workflowExecutionRepositoryPort: WorkflowExecutionRepositoryPort,
  private val waitingIndexPort: WaitingIndexPort,
  private val memberEventStatsPort: MemberEventStatsPort,
  private val actionExecutor: ActionExecutor,
) {

  fun start(workflow: Workflow, tenantId: String, memberId: String?, context: Map<String, Any?>): WorkflowInstance {
    val triggerNode = workflow.definitionJson.nodes.first { it is Node.Trigger } as Node.Trigger
    val instance = workflowInstanceRepositoryPort.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(),
        workflowId = workflow.id,
        tenantId = tenantId,
        triggerEventCode = workflow.triggerEventCode,
        version = workflow.version,
        memberId = memberId,
        status = WorkflowInstanceStatus.RUNNING,
        startedAt = LocalDateTime.now(),
      ),
    )
    return runFrom(instance, workflow, triggerNode.id, context)
  }

  fun resumeOnMatch(instance: WorkflowInstance, workflow: Workflow, eventContext: Map<String, Any?>): WorkflowInstance {
    val node = currentNode(workflow, instance) as? Node.WaitForEvent
      ?: error("instance ${instance.id} is not waiting on a WaitForEvent node")
    val matched = node.event.matchConditions?.let {
      ConditionEvaluator.evaluate(it, eventContext, statsResolverFor(instance.tenantId), instance.memberId)
    } ?: true
    if (!matched) return instance

    completeRunningExecution(instance, node.id)
    waitingIndexPort.remove(instance.tenantId, node.event.eventCode, instance.memberId ?: "", instance.id)
    val nextNodeId = nextNodeId(workflow, node.id, EdgeRoute.Matched)
    return runFrom(instance, workflow, nextNodeId, eventContext)
  }

  fun resumeOnTimeout(instance: WorkflowInstance, workflow: Workflow): WorkflowInstance {
    return when (val node = currentNode(workflow, instance)) {
      is Node.WaitForEvent -> {
        completeRunningExecution(instance, node.id)
        waitingIndexPort.remove(instance.tenantId, node.event.eventCode, instance.memberId ?: "", instance.id)
        runFrom(instance, workflow, nextNodeId(workflow, node.id, EdgeRoute.Timeout), emptyMap())
      }
      is Node.Delay -> {
        completeRunningExecution(instance, node.id)
        runFrom(instance, workflow, nextNodeId(workflow, node.id, EdgeRoute.Always), emptyMap())
      }
      else -> error("instance ${instance.id} is not waiting (currentNodeId=${instance.currentNodeId})")
    }
  }

  private fun runFrom(instanceIn: WorkflowInstance, workflow: Workflow, startNodeId: String, context: Map<String, Any?>): WorkflowInstance {
    val instance = instanceIn
    var nodeId = startNodeId
    while (true) {
      val node = workflow.definitionJson.nodes.first { it.id == nodeId }
      val execution = workflowExecutionRepositoryPort.save(
        WorkflowExecution(
          id = UUID.randomUUID().toString(),
          workflowInstanceId = instance.id,
          nodeId = node.id,
          nodeType = nodeTypeOf(node),
          status = WorkflowExecutionStatus.RUNNING,
          startedAt = LocalDateTime.now(),
        ),
      )

      when (node) {
        is Node.Trigger -> {
          complete(execution)
          nodeId = nextNodeId(workflow, node.id, EdgeRoute.Always)
        }

        is Node.Condition -> {
          val passed = ConditionEvaluator.evaluate(node.condition, context, statsResolverFor(instance.tenantId), instance.memberId)
          complete(execution, result = passed.toString())
          nodeId = nextNodeId(workflow, node.id, if (passed) EdgeRoute.True else EdgeRoute.False)
        }

        is Node.Action -> {
          val result = actionExecutor.execute(node.action)
          complete(execution, result = result)
          nodeId = nextNodeId(workflow, node.id, EdgeRoute.Always)
        }

        is Node.WaitForEvent -> {
          instance.status = WorkflowInstanceStatus.WAITING
          instance.currentNodeId = node.id
          instance.waitingEventName = node.event.eventCode
          instance.waitingUntil = node.event.timeout?.let { LocalDateTime.now().plusNanos(it.toDuration().inWholeNanoseconds) }
          val saved = workflowInstanceRepositoryPort.save(instance)
          waitingIndexPort.register(saved.tenantId, node.event.eventCode, saved.memberId ?: "", saved.id)
          return saved
        }

        is Node.Delay -> {
          instance.status = WorkflowInstanceStatus.WAITING
          instance.currentNodeId = node.id
          instance.waitingUntil = LocalDateTime.now().plusNanos(node.duration.toDuration().inWholeNanoseconds)
          return workflowInstanceRepositoryPort.save(instance)
        }

        is Node.End -> {
          complete(execution)
          instance.status = WorkflowInstanceStatus.COMPLETED
          instance.currentNodeId = node.id
          instance.completedAt = LocalDateTime.now()
          return workflowInstanceRepositoryPort.save(instance)
        }
      }
    }
  }

  private fun complete(execution: WorkflowExecution, result: String? = null) {
    execution.status = WorkflowExecutionStatus.COMPLETED
    execution.completedAt = LocalDateTime.now()
    execution.result = result
    workflowExecutionRepositoryPort.save(execution)
  }

  private fun completeRunningExecution(instance: WorkflowInstance, nodeId: String) {
    val execution = workflowExecutionRepositoryPort.findRunning(instance.id, nodeId) ?: return
    complete(execution)
  }

  private fun currentNode(workflow: Workflow, instance: WorkflowInstance): Node? =
    instance.currentNodeId?.let { id -> workflow.definitionJson.nodes.firstOrNull { it.id == id } }

  private fun nextNodeId(workflow: Workflow, from: String, route: EdgeRoute): String =
    workflow.definitionJson.edges.firstOrNull { it.from == from && it.route == route }?.to
      ?: error("워크플로 ${workflow.id}에 노드 $from 에서 $route 로 가는 엣지가 없습니다")

  private fun nodeTypeOf(node: Node): NodeType = when (node) {
    is Node.Trigger -> NodeType.TRIGGER
    is Node.Condition -> NodeType.CONDITION
    is Node.Action -> NodeType.ACTION
    is Node.WaitForEvent -> NodeType.WAIT_FOR_EVENT
    is Node.Delay -> NodeType.DELAY
    is Node.End -> NodeType.END
  }

  private fun statsResolverFor(tenantId: String): EventStatsResolver =
    EventStatsResolver { memberId, eventCode, withinDays -> memberEventStatsPort.countEvents(tenantId, memberId, eventCode, withinDays) }
}
```

- [ ] **Step 5: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-application:test --tests "com.seaotter.triggerly.application.engine.WorkflowEngineTest"`
Expected: `BUILD SUCCESSFUL`, 4 tests passed

- [ ] **Step 6: Commit**

```bash
git add triggerly-application
git commit -m "feat(application): implement WorkflowEngine covering all 6 node types"
```

---

### Task 6: 유스케이스

**Files:**
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCase.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageEventDefinitionUseCase.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageAttributeDefinitionUseCase.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageMemberUseCase.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/ManageWorkflowUseCase.kt`
- Create: `triggerly-application/src/main/kotlin/com/seaotter/triggerly/application/usecase/WorkflowInstanceQueryUseCase.kt`
- Test: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/IngestEventUseCaseTest.kt`
- Test: `triggerly-application/src/test/kotlin/com/seaotter/triggerly/application/usecase/ManageWorkflowUseCaseTest.kt`

**Interfaces:**
- Consumes: 모든 포트(Task 4), `WorkflowEngine`(Task 5)
- Produces: `IngestEventUseCase.handle(message: RawEventMessage)` — Task 16(Kafka 컨슈머)이 그대로 호출. `ManageWorkflowUseCase`/`ManageEventDefinitionUseCase`/`ManageAttributeDefinitionUseCase`/`ManageMemberUseCase`/`WorkflowInstanceQueryUseCase`의 public 메서드 — Task 18~20(REST 컨트롤러)이 그대로 호출.

- [ ] **Step 1: `IngestEventUseCaseTest.kt` 작성 (mockk)**

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.engine.WorkflowEngine
import com.seaotter.triggerly.application.port.*
import com.seaotter.triggerly.domain.*
import io.mockk.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import java.time.LocalDateTime

class IngestEventUseCaseTest {

  private val memberCommandPort = mockk<MemberCommandPort>()
  private val eventInstanceRepositoryPort = mockk<EventInstanceRepositoryPort>(relaxed = true)
  private val workflowRepositoryPort = mockk<WorkflowRepositoryPort>()
  private val workflowInstanceRepositoryPort = mockk<WorkflowInstanceRepositoryPort>(relaxed = true)
  private val waitingIndexPort = mockk<WaitingIndexPort>()
  private val workflowEngine = mockk<WorkflowEngine>()

  private val useCase = IngestEventUseCase(
    memberCommandPort,
    eventInstanceRepositoryPort,
    workflowRepositoryPort,
    workflowInstanceRepositoryPort,
    waitingIndexPort,
    workflowEngine,
  )

  @BeforeTest
  fun setUp() {
    every { waitingIndexPort.lookup(any(), any(), any()) } returns emptyList()
  }

  @Test
  fun `신규 externalMemberId는 새 Member를 생성한 뒤 트리거되는 워크플로를 시작한다`() {
    val message = RawEventMessage(
      tenantId = "t1",
      eventCode = "SIGN_UP",
      externalMemberId = "ext-1",
      memberContext = MemberContext(tenantId = "t1", externalMemberId = "ext-1", email = "a@b.com"),
      attributes = null,
      occurredAt = LocalDateTime.now(),
    )
    every { memberCommandPort.findByExternalId("t1", "ext-1") } returns null
    val savedMemberSlot = slot<Member>()
    every { memberCommandPort.save(capture(savedMemberSlot)) } answers { savedMemberSlot.captured }

    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "SIGN_UP") } returns listOf(workflow)
    every { workflowEngine.start(workflow, "t1", any(), any()) } returns mockk()

    useCase.handle(message)

    verify { eventInstanceRepositoryPort.save(match { it.eventCode == "SIGN_UP" && it.tenantId == "t1" }) }
    verify { workflowEngine.start(workflow, "t1", savedMemberSlot.captured.id, any()) }
    assertEquals("a@b.com", savedMemberSlot.captured.email)
  }

  @Test
  fun `대기 인덱스에 매칭되는 인스턴스가 있으면 resumeOnMatch를 호출한다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "REVIEW_ADD", externalMemberId = "ext-1",
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
    )
    val existingMember = Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1")
    every { memberCommandPort.findByExternalId("t1", "ext-1") } returns existingMember
    every { memberCommandPort.save(any()) } returns existingMember
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "REVIEW_ADD") } returns emptyList()
    every { waitingIndexPort.lookup("t1", "REVIEW_ADD", "m1") } returns listOf("instance-1")

    val waitingInstance = mockk<WorkflowInstance> { every { workflowId } returns "wf-1" }
    every { workflowInstanceRepositoryPort.findById("instance-1") } returns waitingInstance
    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findById("t1", "wf-1") } returns workflow
    every { workflowEngine.resumeOnMatch(waitingInstance, workflow, any()) } returns mockk()

    useCase.handle(message)

    verify { workflowEngine.resumeOnMatch(waitingInstance, workflow, any()) }
  }

  @Test
  fun `syntheticTimeoutForInstanceId가 있으면 WAITING 인스턴스만 resumeOnTimeout으로 재개한다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "__TIMEOUT__", externalMemberId = null,
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      syntheticTimeoutForInstanceId = "instance-2",
    )
    val waitingInstance = mockk<WorkflowInstance> {
      every { status } returns WorkflowInstanceStatus.WAITING
      every { tenantId } returns "t1"
      every { workflowId } returns "wf-1"
    }
    every { workflowInstanceRepositoryPort.findById("instance-2") } returns waitingInstance
    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findById("t1", "wf-1") } returns workflow
    every { workflowEngine.resumeOnTimeout(waitingInstance, workflow) } returns mockk()

    useCase.handle(message)

    verify { workflowEngine.resumeOnTimeout(waitingInstance, workflow) }
    verify(exactly = 0) { eventInstanceRepositoryPort.save(any()) }
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-application:test --tests "com.seaotter.triggerly.application.usecase.IngestEventUseCaseTest"`
Expected: FAIL (컴파일 에러 — `IngestEventUseCase` 없음)

- [ ] **Step 3: `IngestEventUseCase.kt` 구현**

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.WorkflowEngine
import com.seaotter.triggerly.application.port.*
import com.seaotter.triggerly.domain.*
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

@Service
class IngestEventUseCase(
  private val memberCommandPort: MemberCommandPort,
  private val eventInstanceRepositoryPort: EventInstanceRepositoryPort,
  private val workflowRepositoryPort: WorkflowRepositoryPort,
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val waitingIndexPort: WaitingIndexPort,
  private val workflowEngine: WorkflowEngine,
) {

  fun handle(message: RawEventMessage) {
    if (message.syntheticTimeoutForInstanceId != null) {
      handleTimeout(message.syntheticTimeoutForInstanceId)
      return
    }

    val member = resolveOrCreateMember(message.tenantId, message.externalMemberId, message.memberContext)

    eventInstanceRepositoryPort.save(
      EventInstance(
        id = UUID.randomUUID().toString(),
        tenantId = message.tenantId,
        eventCode = message.eventCode,
        occurredAt = message.occurredAt,
        memberId = member?.id,
        attributes = message.attributes,
      ),
    )

    val context = buildContext(message, member)

    workflowRepositoryPort.findEnabledByTriggerEventCode(message.tenantId, message.eventCode)
      .forEach { workflow -> workflowEngine.start(workflow, message.tenantId, member?.id, context) }

    if (member != null) {
      waitingIndexPort.lookup(message.tenantId, message.eventCode, member.id)
        .mapNotNull { workflowInstanceRepositoryPort.findById(it) }
        .forEach { waiting ->
          val workflow = workflowRepositoryPort.findById(message.tenantId, waiting.workflowId) ?: return@forEach
          workflowEngine.resumeOnMatch(waiting, workflow, context)
        }
    }
  }

  private fun handleTimeout(instanceId: String) {
    val instance = workflowInstanceRepositoryPort.findById(instanceId) ?: return
    if (instance.status != WorkflowInstanceStatus.WAITING) return
    val workflow = workflowRepositoryPort.findById(instance.tenantId, instance.workflowId) ?: return
    workflowEngine.resumeOnTimeout(instance, workflow)
  }

  private fun resolveOrCreateMember(tenantId: String, externalMemberId: String?, context: MemberContext?): Member? {
    if (externalMemberId == null) return null
    val existing = memberCommandPort.findByExternalId(tenantId, externalMemberId)
    if (existing != null) {
      context?.let { applyContext(existing, it) }
      return memberCommandPort.save(existing)
    }
    return memberCommandPort.save(
      Member(
        id = UUID.randomUUID().toString(),
        tenantId = tenantId,
        externalMemberId = externalMemberId,
        name = context?.name,
        email = context?.email,
        telephone = context?.telephone,
        devicePlatform = context?.devicePlatform,
        gender = context?.gender,
        birthday = context?.birthday,
        status = context?.status ?: MemberStatus.ACTIVE,
        joinedAt = context?.joinedAt ?: LocalDateTime.now(),
        lastLoginAt = context?.lastLoginAt,
        marketingSmsAgreed = context?.marketingSmsAgreed ?: false,
        marketingPushAgreed = context?.marketingPushAgreed ?: false,
        marketingEmailAgreed = context?.marketingEmailAgreed ?: false,
        marketingKakaoAgreed = context?.marketingKakaoAgreed ?: false,
        marketingAgreedAt = context?.marketingAgreedAt,
      ),
    )
  }

  private fun applyContext(member: Member, context: MemberContext) {
    context.email?.let { member.email = it }
    context.telephone?.let { member.telephone = it }
    context.devicePlatform?.let { member.devicePlatform = it }
    context.birthday?.let { member.birthday = it }
    context.status?.let { member.status = it }
    context.lastLoginAt?.let { member.lastLoginAt = it }
  }

  private fun buildContext(message: RawEventMessage, member: Member?): Map<String, Any?> {
    val ctx = mutableMapOf<String, Any?>(
      "event.eventCode" to message.eventCode,
      "event.occurredAt" to message.occurredAt,
    )
    message.attributes?.forEach { (k, v) -> ctx["event.$k"] = v }
    if (member != null) {
      ctx["member.id"] = member.id
      ctx["member.email"] = member.email
      ctx["member.telephone"] = member.telephone
      ctx["member.devicePlatform"] = member.devicePlatform?.name
      ctx["member.gender"] = member.gender?.name
      ctx["member.birthday"] = member.birthday
      ctx["member.status"] = member.status?.name
      val today = LocalDate.now()
      ctx["member.isBirthdayToday"] = member.birthday?.let { it.monthValue == today.monthValue && it.dayOfMonth == today.dayOfMonth } ?: false
      member.attributes?.forEach { (k, v) -> ctx["member.attributes.$k"] = v }
    }
    return ctx
  }
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-application:test --tests "com.seaotter.triggerly.application.usecase.IngestEventUseCaseTest"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed

- [ ] **Step 5: 나머지 유스케이스 구현 (CRUD 성격 — 각각 짧으므로 한 스텝에서 함께 작성)**

`ManageEventDefinitionUseCase.kt`:
```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.stereotype.Service

@Service
class ManageEventDefinitionUseCase(private val port: EventDefinitionPort) {
  fun register(tenantId: String, code: String, displayName: String): EventDefinition =
    port.save(EventDefinition(tenantId = tenantId, code = code, displayName = displayName))

  fun list(tenantId: String): List<EventDefinition> = port.findAll(tenantId)
}
```

`ManageAttributeDefinitionUseCase.kt`:
```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.AttributeDefinitionPort
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ManageAttributeDefinitionUseCase(private val port: AttributeDefinitionPort) {
  fun register(
    tenantId: String,
    eventDefinitionId: String?,
    key: String,
    displayName: String,
    type: AttributeType,
    filterable: Boolean,
  ): AttributeDefinition = port.save(
    AttributeDefinition(
      id = UUID.randomUUID().toString(),
      tenantId = tenantId,
      eventDefinitionId = eventDefinitionId,
      key = key,
      displayName = displayName,
      type = type,
      filterable = filterable,
    ),
  )

  fun list(tenantId: String): List<AttributeDefinition> = port.findAll(tenantId)
}
```

`ManageMemberUseCase.kt`:
```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.MemberCommandPort
import com.seaotter.triggerly.application.port.MemberQueryPort
import com.seaotter.triggerly.domain.Member
import org.springframework.stereotype.Service

@Service
class ManageMemberUseCase(
  private val memberCommandPort: MemberCommandPort,
  private val memberQueryPort: MemberQueryPort,
) {
  fun register(member: Member): Member = memberCommandPort.save(member)
  fun search(tenantId: String, criteria: Map<String, Any?>, limit: Int = 50): List<Member> =
    memberQueryPort.search(tenantId, criteria, limit)
}
```

`ManageWorkflowUseCase.kt`:
```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowStatus
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ManageWorkflowUseCase(private val port: WorkflowRepositoryPort) {
  fun create(workflow: Workflow): Workflow = port.save(workflow)
  fun list(tenantId: String): List<Workflow> = port.findAll(tenantId)
  fun get(tenantId: String, id: String): Workflow? = port.findById(tenantId, id)

  fun enable(tenantId: String, id: String): Workflow {
    val workflow = port.findById(tenantId, id) ?: error("workflow not found: $id")
    workflow.status = WorkflowStatus.ENABLED
    workflow.lastUpdatedAt = LocalDateTime.now()
    return port.save(workflow)
  }
}
```

`WorkflowInstanceQueryUseCase.kt`:
```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowInstance
import org.springframework.stereotype.Service

data class WorkflowInstanceView(val instance: WorkflowInstance, val executions: List<WorkflowExecution>)

@Service
class WorkflowInstanceQueryUseCase(
  private val instancePort: WorkflowInstanceRepositoryPort,
  private val executionPort: WorkflowExecutionRepositoryPort,
) {
  fun get(id: String): WorkflowInstanceView? {
    val instance = instancePort.findById(id) ?: return null
    return WorkflowInstanceView(instance, executionPort.findByInstanceId(id))
  }
}
```

- [ ] **Step 6: `ManageWorkflowUseCaseTest.kt` 작성 및 통과 확인 (enable 로직 검증)**

```kotlin
package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import java.time.LocalDateTime

class ManageWorkflowUseCaseTest {

  @Test
  fun `enable은 워크플로 상태를 ENABLED로 바꾸고 저장한다`() {
    val port = mockk<WorkflowRepositoryPort>()
    val workflow = Workflow(
      id = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
      definitionJson = WorkflowDefinition("LOGIN", emptyList(), emptyList()),
      status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )
    every { port.findById("t1", "wf-1") } returns workflow
    every { port.save(any()) } answers { firstArg() }

    val useCase = ManageWorkflowUseCase(port)
    val result = useCase.enable("t1", "wf-1")

    assertEquals(WorkflowStatus.ENABLED, result.status)
    verify { port.save(workflow) }
  }
}
```

Run: `./gradlew :triggerly-application:test --tests "com.seaotter.triggerly.application.usecase.ManageWorkflowUseCaseTest"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 7: 전체 application 모듈 테스트**

Run: `./gradlew :triggerly-application:test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 8: Commit**

```bash
git add triggerly-application
git commit -m "feat(application): add ingest/manage use cases orchestrating the workflow engine"
```

---

### Task 7: Flyway 마이그레이션 + JSON 컨버터 + EventDefinition/AttributeDefinition MySQL 어댑터

**Files:**
- Create: `triggerly-bootstrap/src/main/resources/db/migration/V1__init.sql`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/json/JsonMapper.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/json/MapJsonConverter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/json/WorkflowDefinitionJsonConverter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventDefinitionEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventDefinitionJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventDefinitionPersistenceAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/AttributeDefinitionEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/AttributeDefinitionJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/AttributeDefinitionPersistenceAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/PersistenceTestApplication.kt`
- Create: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/MySqlIntegrationTest.kt`
- Create: `triggerly-adapter-out-persistence/src/test/resources/application.yml`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventDefinitionPersistenceAdapterTest.kt`

**Interfaces:**
- Consumes: `EventDefinitionPort`, `AttributeDefinitionPort` (Task 4), 도메인 타입 (Task 2)
- Produces: `JsonMapper.instance`(공유 Jackson `ObjectMapper`), `MapJsonConverter`, `WorkflowDefinitionJsonConverter` — Task 8~10이 그대로 재사용. `PersistenceTestApplication`, `MySqlIntegrationTest`(추상 베이스, `@ServiceConnection`으로 Testcontainers MySQL 자동 연결) — Task 8~10의 MySQL 테스트가 상속.
- **설계 결정**: 실제 앱(부트스트랩)은 Flyway로 스키마를 만들고 `ddl-auto: validate`로 검증하지만(Task 21), `adapter-out-persistence` 모듈 자체의 슬라이스 테스트는 부트스트랩의 마이그레이션 리소스에 접근할 수 없으므로(모듈 의존 방향상 불가) `ddl-auto: create-drop`으로 엔티티에서 스키마를 자동 생성해 테스트한다. 두 방식이 어긋나지 않도록 엔티티 필드는 `V1__init.sql`의 컬럼과 정확히 대응시킨다.

- [ ] **Step 1: `V1__init.sql` 작성 (스펙 5.1절의 7개 테이블)**

```sql
CREATE TABLE event_definition (
  id VARCHAR(150) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  code VARCHAR(100) NOT NULL,
  display_name VARCHAR(200) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uk_event_definition_tenant_code (tenant_id, code)
);

CREATE TABLE attribute_definition (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  event_definition_id VARCHAR(150) NULL,
  attr_key VARCHAR(100) NOT NULL,
  display_name VARCHAR(200) NOT NULL,
  attr_type VARCHAR(20) NOT NULL,
  filterable BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME NOT NULL,
  KEY idx_attribute_definition_tenant (tenant_id)
);

CREATE TABLE member (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  external_member_id VARCHAR(100) NOT NULL,
  name VARCHAR(100) NULL,
  email VARCHAR(200) NULL,
  telephone VARCHAR(30) NULL,
  device_platform VARCHAR(20) NULL,
  gender VARCHAR(10) NULL,
  birthday DATE NULL,
  status VARCHAR(20) NULL,
  joined_at DATETIME NULL,
  last_login_at DATETIME NULL,
  withdrawn_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  marketing_sms_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_push_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_email_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_kakao_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_agreed_at DATETIME NULL,
  attributes JSON NULL,
  UNIQUE KEY uk_member_tenant_external (tenant_id, external_member_id)
);

CREATE TABLE event_instance (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  event_code VARCHAR(100) NOT NULL,
  occurred_at DATETIME NOT NULL,
  member_id VARCHAR(36) NULL,
  attributes JSON NULL,
  KEY idx_event_instance_occurred_at (occurred_at)
);

CREATE TABLE workflow (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  name VARCHAR(200) NULL,
  trigger_event_code VARCHAR(100) NOT NULL,
  definition_json JSON NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  status VARCHAR(20) NOT NULL,
  created_at DATETIME NOT NULL,
  last_updated_at DATETIME NOT NULL,
  KEY idx_workflow_tenant_trigger (tenant_id, trigger_event_code, status)
);

CREATE TABLE workflow_instance (
  id VARCHAR(36) PRIMARY KEY,
  workflow_id VARCHAR(36) NOT NULL,
  tenant_id VARCHAR(50) NOT NULL,
  trigger_event_code VARCHAR(100) NOT NULL,
  version BIGINT NOT NULL,
  member_id VARCHAR(36) NULL,
  status VARCHAR(20) NOT NULL,
  current_node_id VARCHAR(50) NULL,
  waiting_event_name VARCHAR(100) NULL,
  waiting_until DATETIME NULL,
  started_at DATETIME NULL,
  completed_at DATETIME NULL,
  KEY idx_workflow_instance_waiting (status, waiting_until)
);

CREATE TABLE workflow_execution (
  id VARCHAR(36) PRIMARY KEY,
  workflow_instance_id VARCHAR(36) NOT NULL,
  node_id VARCHAR(50) NOT NULL,
  node_type VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL,
  started_at DATETIME NULL,
  completed_at DATETIME NULL,
  result VARCHAR(500) NULL,
  error_code VARCHAR(100) NULL,
  KEY idx_workflow_execution_instance_node (workflow_instance_id, node_id)
);
```

- [ ] **Step 2: JSON 컨버터 3종 작성**

```kotlin
package com.seaotter.triggerly.adapter.persistence.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object JsonMapper {
  val instance: ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.json

import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter

@Converter
class MapJsonConverter : AttributeConverter<Map<String, Any?>?, String?> {
  override fun convertToDatabaseColumn(attribute: Map<String, Any?>?): String? =
    attribute?.let { JsonMapper.instance.writeValueAsString(it) }

  override fun convertToEntityAttribute(dbData: String?): Map<String, Any?>? =
    dbData?.let { JsonMapper.instance.readValue<Map<String, Any?>>(it) }
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.json

import com.fasterxml.jackson.module.kotlin.readValue
import com.seaotter.triggerly.domain.WorkflowDefinition
import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter

@Converter
class WorkflowDefinitionJsonConverter : AttributeConverter<WorkflowDefinition, String> {
  override fun convertToDatabaseColumn(attribute: WorkflowDefinition): String =
    JsonMapper.instance.writeValueAsString(attribute)

  override fun convertToEntityAttribute(dbData: String): WorkflowDefinition =
    JsonMapper.instance.readValue(dbData)
}
```

- [ ] **Step 3: 테스트 인프라 (`PersistenceTestApplication`, `MySqlIntegrationTest`, `application.yml`)**

```kotlin
package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class PersistenceTestApplication
```

```kotlin
package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@SpringBootTest(classes = [PersistenceTestApplication::class])
abstract class MySqlIntegrationTest {
  companion object {
    @Container
    @ServiceConnection
    @JvmStatic
    val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4").apply { withDatabaseName("triggerly") }
  }
}
```

`triggerly-adapter-out-persistence/src/test/resources/application.yml`:
```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: create-drop
    properties:
      hibernate:
        format_sql: true
```

- [ ] **Step 4: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.EventDefinition
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertEquals
import kotlin.test.assertNull

class EventDefinitionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: EventDefinitionPersistenceAdapter

  @Test
  fun `저장한 EventDefinition을 tenantId와 code로 조회할 수 있다`() {
    adapter.save(EventDefinition(tenantId = "t1", code = "PURCHASE", displayName = "구매"))

    val found = adapter.findByTenantAndCode("t1", "PURCHASE")

    assertEquals("구매", found?.displayName)
    assertNull(adapter.findByTenantAndCode("t1", "UNKNOWN"))
  }

  @Test
  fun `findAll은 테넌트별로만 조회한다`() {
    adapter.save(EventDefinition(tenantId = "t1", code = "A", displayName = "A"))
    adapter.save(EventDefinition(tenantId = "t2", code = "A", displayName = "A"))

    assertEquals(1, adapter.findAll("t1").size)
  }
}
```

- [ ] **Step 5: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*EventDefinitionPersistenceAdapterTest*"`
Expected: FAIL (컴파일 에러 — 엔티티/리포지토리/어댑터 없음. Docker 데몬이 꺼져 있으면 Testcontainers가 컨테이너를 못 띄워 실패할 수 있으니 `docker ps`로 먼저 확인)

- [ ] **Step 6: `EventDefinitionEntity`/`Repository`/`Adapter` 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "event_definition")
class EventDefinitionEntity(
  @Id var id: String,
  var tenantId: String,
  var code: String,
  var displayName: String,
  var createdAt: LocalDateTime,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface EventDefinitionJpaRepository : JpaRepository<EventDefinitionEntity, String> {
  fun findByTenantIdAndCode(tenantId: String, code: String): EventDefinitionEntity?
  fun findAllByTenantId(tenantId: String): List<EventDefinitionEntity>
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.stereotype.Component

@Component
class EventDefinitionPersistenceAdapter(
  private val repository: EventDefinitionJpaRepository,
) : EventDefinitionPort {

  override fun save(definition: EventDefinition): EventDefinition {
    val entity = EventDefinitionEntity(
      id = definition.id,
      tenantId = definition.tenantId,
      code = definition.code,
      displayName = definition.displayName,
      createdAt = definition.createdAt,
    )
    return repository.save(entity).toDomain()
  }

  override fun findByTenantAndCode(tenantId: String, code: String): EventDefinition? =
    repository.findByTenantIdAndCode(tenantId, code)?.toDomain()

  override fun findAll(tenantId: String): List<EventDefinition> =
    repository.findAllByTenantId(tenantId).map { it.toDomain() }

  private fun EventDefinitionEntity.toDomain() = EventDefinition(tenantId, code, displayName, createdAt, id)
}
```

- [ ] **Step 7: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*EventDefinitionPersistenceAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 8: `AttributeDefinition` 엔티티/리포지토리/어댑터 (같은 패턴)**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "attribute_definition")
class AttributeDefinitionEntity(
  @Id var id: String,
  var tenantId: String,
  var eventDefinitionId: String?,
  var attrKey: String,
  var displayName: String,
  var attrType: String,
  var filterable: Boolean,
  var createdAt: LocalDateTime,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface AttributeDefinitionJpaRepository : JpaRepository<AttributeDefinitionEntity, String> {
  fun findAllByTenantId(tenantId: String): List<AttributeDefinitionEntity>
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.AttributeDefinitionPort
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import org.springframework.stereotype.Component

@Component
class AttributeDefinitionPersistenceAdapter(
  private val repository: AttributeDefinitionJpaRepository,
) : AttributeDefinitionPort {

  override fun save(definition: AttributeDefinition): AttributeDefinition {
    val entity = AttributeDefinitionEntity(
      id = definition.id,
      tenantId = definition.tenantId,
      eventDefinitionId = definition.eventDefinitionId,
      attrKey = definition.key,
      displayName = definition.displayName,
      attrType = definition.type.name,
      filterable = definition.filterable,
      createdAt = definition.createdAt,
    )
    return repository.save(entity).toDomain()
  }

  override fun findAll(tenantId: String): List<AttributeDefinition> =
    repository.findAllByTenantId(tenantId).map { it.toDomain() }

  private fun AttributeDefinitionEntity.toDomain() = AttributeDefinition(
    id = id, tenantId = tenantId, eventDefinitionId = eventDefinitionId, key = attrKey,
    displayName = displayName, type = AttributeType.valueOf(attrType), filterable = filterable, createdAt = createdAt,
  )
}
```

- [ ] **Step 9: `AttributeDefinitionPersistenceAdapterTest.kt` 작성 및 통과 확인**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals

class AttributeDefinitionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: AttributeDefinitionPersistenceAdapter

  @Test
  fun `저장한 AttributeDefinition을 테넌트별로 조회할 수 있다`() {
    adapter.save(
      AttributeDefinition(
        id = UUID.randomUUID().toString(), tenantId = "t1", key = "amount",
        displayName = "결제 금액", type = AttributeType.LONG, filterable = true,
      ),
    )
    val list = adapter.findAll("t1")
    assertEquals(1, list.size)
    assertEquals(AttributeType.LONG, list[0].type)
  }
}
```

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*AttributeDefinitionPersistenceAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 10: Commit**

```bash
git add triggerly-bootstrap/src/main/resources/db/migration triggerly-adapter-out-persistence
git commit -m "feat(persistence): add Flyway schema, JSON converters, EventDefinition/AttributeDefinition adapters"
```

---

### Task 8: Member MySQL 어댑터

**Files:**
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/MemberSavedEvent.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/MemberEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/MemberJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/MemberPersistenceAdapter.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/MemberPersistenceAdapterTest.kt`

**Interfaces:**
- Consumes: `MemberCommandPort` (Task 4), `Member`/`MemberStatus`/`Gender`/`DevicePlatform` (Task 2), `MapJsonConverter`(Task 7), `MySqlIntegrationTest`(Task 7)
- Produces: `MemberSavedEvent(member: Member)` — Task 11(ES 비동기 동기화)이 `@EventListener`로 구독한다.

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.Member
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MemberPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: MemberPersistenceAdapter

  @Test
  fun `Member를 저장하고 externalMemberId로 다시 조회하면 attributes까지 복원된다`() {
    val member = Member(
      id = UUID.randomUUID().toString(),
      tenantId = "t1",
      externalMemberId = "ext-1",
      email = "a@b.com",
      attributes = mapOf("grade" to "VIP"),
    )
    adapter.save(member)

    val found = adapter.findByExternalId("t1", "ext-1")

    assertEquals("a@b.com", found?.email)
    assertEquals("VIP", found?.attributes?.get("grade"))
    assertNull(adapter.findByExternalId("t1", "unknown"))
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*MemberPersistenceAdapterTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence

import com.seaotter.triggerly.domain.Member

data class MemberSavedEvent(val member: Member)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.json.MapJsonConverter
import jakarta.persistence.Column
import jakarta.persistence.Convert
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDate
import java.time.LocalDateTime

@Entity
@Table(name = "member")
class MemberEntity(
  @Id var id: String,
  var tenantId: String,
  var externalMemberId: String,
  var name: String?,
  var email: String?,
  var telephone: String?,
  var devicePlatform: String?,
  var gender: String?,
  var birthday: LocalDate?,
  var status: String?,
  var joinedAt: LocalDateTime?,
  var lastLoginAt: LocalDateTime?,
  var withdrawnAt: LocalDateTime?,
  var createdAt: LocalDateTime,
  var marketingSmsAgreed: Boolean,
  var marketingPushAgreed: Boolean,
  var marketingEmailAgreed: Boolean,
  var marketingKakaoAgreed: Boolean,
  var marketingAgreedAt: LocalDateTime?,
  @Column(columnDefinition = "JSON")
  @Convert(converter = MapJsonConverter::class)
  var attributes: Map<String, Any?>?,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface MemberJpaRepository : JpaRepository<MemberEntity, String> {
  fun findByTenantIdAndExternalMemberId(tenantId: String, externalMemberId: String): MemberEntity?
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MemberSavedEvent
import com.seaotter.triggerly.application.port.MemberCommandPort
import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.Member
import com.seaotter.triggerly.domain.MemberStatus
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
class MemberPersistenceAdapter(
  private val repository: MemberJpaRepository,
  private val eventPublisher: ApplicationEventPublisher,
) : MemberCommandPort {

  override fun save(member: Member): Member {
    val saved = repository.save(member.toEntity()).toDomain()
    eventPublisher.publishEvent(MemberSavedEvent(saved))
    return saved
  }

  override fun findById(id: String): Member? = repository.findById(id).orElse(null)?.toDomain()

  override fun findByExternalId(tenantId: String, externalMemberId: String): Member? =
    repository.findByTenantIdAndExternalMemberId(tenantId, externalMemberId)?.toDomain()

  private fun Member.toEntity() = MemberEntity(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.name, gender = gender?.name, birthday = birthday,
    status = status?.name, joinedAt = joinedAt, lastLoginAt = lastLoginAt, withdrawnAt = withdrawnAt,
    createdAt = createdAt, marketingSmsAgreed = marketingSmsAgreed, marketingPushAgreed = marketingPushAgreed,
    marketingEmailAgreed = marketingEmailAgreed, marketingKakaoAgreed = marketingKakaoAgreed,
    marketingAgreedAt = marketingAgreedAt, attributes = attributes,
  )

  private fun MemberEntity.toDomain() = Member(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.let { DevicePlatform.valueOf(it) },
    gender = gender?.let { Gender.valueOf(it) }, birthday = birthday, status = status?.let { MemberStatus.valueOf(it) },
    joinedAt = joinedAt, lastLoginAt = lastLoginAt, withdrawnAt = withdrawnAt, createdAt = createdAt,
    marketingSmsAgreed = marketingSmsAgreed, marketingPushAgreed = marketingPushAgreed,
    marketingEmailAgreed = marketingEmailAgreed, marketingKakaoAgreed = marketingKakaoAgreed,
    marketingAgreedAt = marketingAgreedAt, attributes = attributes,
  )
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*MemberPersistenceAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-adapter-out-persistence
git commit -m "feat(persistence): add Member MySQL adapter with MemberSavedEvent"
```

---

### Task 9: EventInstance MySQL 스토어

**Files:**
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventInstanceEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventInstanceJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventInstanceMySqlStore.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/EventInstanceMySqlStoreTest.kt`

**Interfaces:**
- Consumes: `EventInstance`(Task 2), `MapJsonConverter`(Task 7)
- Produces: `EventInstanceMySqlStore.save(instance): EventInstance`, `EventInstanceMySqlStore.deleteOlderThan(cutoff): Int` — `EventInstanceRepositoryPort`을 구현하는 Task 12의 `EventInstanceRepositoryAdapter`가 이 컴포넌트를 주입받아 MySQL 쪽 절반을 위임한다. 이 태스크 자체는 포트를 구현하지 않는다(포트 구현은 ES 색인까지 갖춰지는 Task 12에서 완성).

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.EventInstance
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class EventInstanceMySqlStoreTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var store: EventInstanceMySqlStore

  @Test
  fun `저장한 EventInstance를 attributes까지 그대로 복원한다`() {
    val saved = store.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "PURCHASE",
        occurredAt = LocalDateTime.now(), memberId = "m1", attributes = mapOf("amount" to 50000),
      ),
    )
    assertEquals(50000, saved.attributes?.get("amount"))
  }

  @Test
  fun `deleteOlderThan은 cutoff보다 오래된 행만 지운다`() {
    val old = store.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "OLD",
        occurredAt = LocalDateTime.now().minusDays(31),
      ),
    )
    val recent = store.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "RECENT",
        occurredAt = LocalDateTime.now(),
      ),
    )

    val deleted = store.deleteOlderThan(LocalDateTime.now().minusDays(30))

    assertEquals(1, deleted)
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*EventInstanceMySqlStoreTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.json.MapJsonConverter
import jakarta.persistence.Column
import jakarta.persistence.Convert
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "event_instance")
class EventInstanceEntity(
  @Id var id: String,
  var tenantId: String,
  var eventCode: String,
  var occurredAt: LocalDateTime,
  var memberId: String?,
  @Column(columnDefinition = "JSON")
  @Convert(converter = MapJsonConverter::class)
  var attributes: Map<String, Any?>?,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.time.LocalDateTime

interface EventInstanceJpaRepository : JpaRepository<EventInstanceEntity, String> {
  @Modifying
  @Query("DELETE FROM EventInstanceEntity e WHERE e.occurredAt < :cutoff")
  fun deleteByOccurredAtBefore(cutoff: LocalDateTime): Int
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.domain.EventInstance
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Component
class EventInstanceMySqlStore(private val repository: EventInstanceJpaRepository) {

  fun save(instance: EventInstance): EventInstance {
    val entity = EventInstanceEntity(
      id = instance.id, tenantId = instance.tenantId, eventCode = instance.eventCode,
      occurredAt = instance.occurredAt, memberId = instance.memberId, attributes = instance.attributes,
    )
    return repository.save(entity).toDomain()
  }

  @Transactional
  fun deleteOlderThan(cutoff: LocalDateTime): Int = repository.deleteByOccurredAtBefore(cutoff)

  private fun EventInstanceEntity.toDomain() = EventInstance(id, tenantId, eventCode, occurredAt, memberId, attributes)
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*EventInstanceMySqlStoreTest*"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-adapter-out-persistence
git commit -m "feat(persistence): add EventInstance MySQL store with 30-day cleanup query"
```

---

### Task 10: Workflow/WorkflowInstance/WorkflowExecution MySQL 어댑터

**Files:**
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowPersistenceAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstanceEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstanceJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowExecutionEntity.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowExecutionJpaRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowExecutionPersistenceAdapter.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowPersistenceAdapterTest.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowInstancePersistenceAdapterTest.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/jpa/WorkflowExecutionPersistenceAdapterTest.kt`

**Interfaces:**
- Consumes: `WorkflowRepositoryPort`, `WorkflowInstanceRepositoryPort`, `WorkflowExecutionRepositoryPort` (Task 4), `WorkflowDefinitionJsonConverter`(Task 7), 도메인 타입(Task 2)
- Produces: 세 포트의 완전한 MySQL 구현 — Task 16(Kafka 컨슈머)과 Task 21(스케줄러)이 `WorkflowInstanceRepositoryPort.findWaitingExpired`를, Task 5(엔진)가 나머지를 이미 소비 중.

- [ ] **Step 1: 실패하는 테스트 3개 작성**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkflowPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: WorkflowPersistenceAdapter

  private fun sampleWorkflow(id: String, status: WorkflowStatus) = Workflow(
    id = id, tenantId = "t1", triggerEventCode = "LOGIN",
    definitionJson = WorkflowDefinition("LOGIN", listOf(Node.Trigger("n1", "LOGIN"), Node.End("n2")), listOf(Edge("n1", "n2", EdgeRoute.Always))),
    status = status, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )

  @Test
  fun `definitionJson을 저장하고 복원하면 노드-엣지 구조가 그대로 유지된다`() {
    adapter.save(sampleWorkflow("wf-1", WorkflowStatus.ENABLED))
    val found = adapter.findById("t1", "wf-1")
    assertEquals(2, found?.definitionJson?.nodes?.size)
    assertTrue(found?.definitionJson?.nodes?.get(0) is Node.Trigger)
  }

  @Test
  fun `findEnabledByTriggerEventCode는 ENABLED 상태만 반환한다`() {
    adapter.save(sampleWorkflow("wf-2", WorkflowStatus.ENABLED))
    adapter.save(sampleWorkflow("wf-3", WorkflowStatus.DRAFT))
    val enabled = adapter.findEnabledByTriggerEventCode("t1", "LOGIN")
    assertEquals(1, enabled.size)
    assertEquals("wf-2", enabled[0].id)
  }
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class WorkflowInstancePersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: WorkflowInstancePersistenceAdapter

  @Test
  fun `findWaitingExpired는 waitingUntil이 지난 WAITING 인스턴스만 반환한다`() {
    val expired = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
      ),
    )
    adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().plusMinutes(10),
      ),
    )

    val result = adapter.findWaitingExpired(LocalDateTime.now())

    assertEquals(1, result.size)
    assertEquals(expired.id, result[0].id)
  }
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.NodeType
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowExecutionStatus
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull

class WorkflowExecutionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: WorkflowExecutionPersistenceAdapter

  @Test
  fun `findRunning은 RUNNING 상태인 실행 기록만 찾는다`() {
    val instanceId = UUID.randomUUID().toString()
    adapter.save(WorkflowExecution(UUID.randomUUID().toString(), instanceId, "n1", NodeType.TRIGGER, WorkflowExecutionStatus.COMPLETED))
    val running = adapter.save(WorkflowExecution(UUID.randomUUID().toString(), instanceId, "n2", NodeType.WAIT_FOR_EVENT, WorkflowExecutionStatus.RUNNING))

    val found = adapter.findRunning(instanceId, "n2")
    assertEquals(running.id, found?.id)
    assertNull(adapter.findRunning(instanceId, "n1"))
    assertEquals(2, adapter.findByInstanceId(instanceId).size)
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*WorkflowPersistenceAdapterTest*" --tests "*WorkflowInstancePersistenceAdapterTest*" --tests "*WorkflowExecutionPersistenceAdapterTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: `Workflow` 엔티티/리포지토리/어댑터 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.json.WorkflowDefinitionJsonConverter
import com.seaotter.triggerly.domain.WorkflowDefinition
import jakarta.persistence.Column
import jakarta.persistence.Convert
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "workflow")
class WorkflowEntity(
  @Id var id: String,
  var tenantId: String,
  var name: String?,
  var triggerEventCode: String,
  @Column(columnDefinition = "JSON")
  @Convert(converter = WorkflowDefinitionJsonConverter::class)
  var definitionJson: WorkflowDefinition,
  var version: Long,
  var status: String,
  var createdAt: LocalDateTime,
  var lastUpdatedAt: LocalDateTime,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface WorkflowJpaRepository : JpaRepository<WorkflowEntity, String> {
  fun findByIdAndTenantId(id: String, tenantId: String): WorkflowEntity?
  fun findAllByTenantId(tenantId: String): List<WorkflowEntity>
  fun findAllByTenantIdAndTriggerEventCodeAndStatus(tenantId: String, triggerEventCode: String, status: String): List<WorkflowEntity>
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowStatus
import org.springframework.stereotype.Component

@Component
class WorkflowPersistenceAdapter(private val repository: WorkflowJpaRepository) : WorkflowRepositoryPort {

  override fun save(workflow: Workflow): Workflow = repository.save(workflow.toEntity()).toDomain()

  override fun findById(tenantId: String, id: String): Workflow? =
    repository.findByIdAndTenantId(id, tenantId)?.toDomain()

  override fun findAll(tenantId: String): List<Workflow> = repository.findAllByTenantId(tenantId).map { it.toDomain() }

  override fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow> =
    repository.findAllByTenantIdAndTriggerEventCodeAndStatus(tenantId, eventCode, WorkflowStatus.ENABLED.name).map { it.toDomain() }

  private fun Workflow.toEntity() = WorkflowEntity(
    id = id, tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
    definitionJson = definitionJson, version = version, status = status.name,
    createdAt = createdAt, lastUpdatedAt = lastUpdatedAt,
  )

  private fun WorkflowEntity.toDomain() = Workflow(
    id = id, tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
    definitionJson = definitionJson, version = version, status = WorkflowStatus.valueOf(status),
    createdAt = createdAt, lastUpdatedAt = lastUpdatedAt,
  )
}
```

- [ ] **Step 4: `WorkflowInstance` 엔티티/리포지토리/어댑터 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "workflow_instance")
class WorkflowInstanceEntity(
  @Id var id: String,
  var workflowId: String,
  var tenantId: String,
  var triggerEventCode: String,
  var version: Long,
  var memberId: String?,
  var status: String,
  var currentNodeId: String?,
  var waitingEventName: String?,
  var waitingUntil: LocalDateTime?,
  var startedAt: LocalDateTime?,
  var completedAt: LocalDateTime?,
)
```

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
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import org.springframework.data.domain.PageRequest
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class WorkflowInstancePersistenceAdapter(
  private val repository: WorkflowInstanceJpaRepository,
) : WorkflowInstanceRepositoryPort {

  override fun save(instance: WorkflowInstance): WorkflowInstance = repository.save(instance.toEntity()).toDomain()

  override fun findById(id: String): WorkflowInstance? = repository.findById(id).orElse(null)?.toDomain()

  override fun findWaitingExpired(now: LocalDateTime, limit: Int): List<WorkflowInstance> =
    repository.findAllByStatusAndWaitingUntilLessThanEqual(WorkflowInstanceStatus.WAITING.name, now, PageRequest.of(0, limit))
      .map { it.toDomain() }

  private fun WorkflowInstance.toEntity() = WorkflowInstanceEntity(
    id = id, workflowId = workflowId, tenantId = tenantId, triggerEventCode = triggerEventCode, version = version,
    memberId = memberId, status = status.name, currentNodeId = currentNodeId, waitingEventName = waitingEventName,
    waitingUntil = waitingUntil, startedAt = startedAt, completedAt = completedAt,
  )

  private fun WorkflowInstanceEntity.toDomain() = WorkflowInstance(
    id = id, workflowId = workflowId, tenantId = tenantId, triggerEventCode = triggerEventCode, version = version,
    memberId = memberId, status = WorkflowInstanceStatus.valueOf(status), currentNodeId = currentNodeId,
    waitingEventName = waitingEventName, waitingUntil = waitingUntil, startedAt = startedAt, completedAt = completedAt,
  )
}
```

- [ ] **Step 5: `WorkflowExecution` 엔티티/리포지토리/어댑터 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "workflow_execution")
class WorkflowExecutionEntity(
  @Id var id: String,
  var workflowInstanceId: String,
  var nodeId: String,
  var nodeType: String,
  var status: String,
  var startedAt: LocalDateTime?,
  var completedAt: LocalDateTime?,
  var result: String?,
  var errorCode: String?,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface WorkflowExecutionJpaRepository : JpaRepository<WorkflowExecutionEntity, String> {
  fun findAllByWorkflowInstanceId(workflowInstanceId: String): List<WorkflowExecutionEntity>
  fun findFirstByWorkflowInstanceIdAndNodeIdAndStatus(
    workflowInstanceId: String,
    nodeId: String,
    status: String,
  ): WorkflowExecutionEntity?
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.domain.NodeType
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowExecutionStatus
import org.springframework.stereotype.Component

@Component
class WorkflowExecutionPersistenceAdapter(
  private val repository: WorkflowExecutionJpaRepository,
) : WorkflowExecutionRepositoryPort {

  override fun save(execution: WorkflowExecution): WorkflowExecution = repository.save(execution.toEntity()).toDomain()

  override fun findByInstanceId(instanceId: String): List<WorkflowExecution> =
    repository.findAllByWorkflowInstanceId(instanceId).map { it.toDomain() }

  override fun findRunning(instanceId: String, nodeId: String): WorkflowExecution? =
    repository.findFirstByWorkflowInstanceIdAndNodeIdAndStatus(instanceId, nodeId, WorkflowExecutionStatus.RUNNING.name)?.toDomain()

  private fun WorkflowExecution.toEntity() = WorkflowExecutionEntity(
    id = id, workflowInstanceId = workflowInstanceId, nodeId = nodeId, nodeType = nodeType.name,
    status = status.name, startedAt = startedAt, completedAt = completedAt, result = result, errorCode = errorCode,
  )

  private fun WorkflowExecutionEntity.toDomain() = WorkflowExecution(
    id = id, workflowInstanceId = workflowInstanceId, nodeId = nodeId, nodeType = NodeType.valueOf(nodeType),
    status = WorkflowExecutionStatus.valueOf(status), startedAt = startedAt, completedAt = completedAt,
    result = result, errorCode = errorCode,
  )
}
```

- [ ] **Step 6: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*WorkflowPersistenceAdapterTest*" --tests "*WorkflowInstancePersistenceAdapterTest*" --tests "*WorkflowExecutionPersistenceAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 4 tests passed

- [ ] **Step 7: 전체 persistence 모듈 MySQL 관련 테스트 실행**

Run: `./gradlew :triggerly-adapter-out-persistence:test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 8: Commit**

```bash
git add triggerly-adapter-out-persistence
git commit -m "feat(persistence): add Workflow/WorkflowInstance/WorkflowExecution MySQL adapters"
```

---

### Task 11: Elasticsearch 설정 + Member ES 어댑터 + 비동기 동기화

**Files:**
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/AsyncConfig.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberDocument.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberElasticsearchRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberSearchAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberEsSyncListener.kt`
- Create: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/ElasticsearchIntegrationTest.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberEsSyncListenerTest.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberSearchAdapterTest.kt`

**Interfaces:**
- Consumes: `MemberQueryPort`(Task 4), `MemberSavedEvent`(Task 8)
- Produces: `AsyncConfig`(빈 이름 `memberSyncExecutor`, 스펙 13.4절 bounded 풀+CallerRunsPolicy), `ElasticsearchIntegrationTest`(Task 12가 상속)
- **설계 결정**: MySQL 어댑터(Task 8)가 저장 후 `MemberSavedEvent`를 발행하고, 이 태스크의 `MemberEsSyncListener`가 `@Async`로 구독해 ES에 반영한다 — 두 어댑터가 직접 서로를 호출하지 않고 Spring 이벤트로만 연결되어 결합도가 낮다.

- [ ] **Step 1: `AsyncConfig.kt` 작성 (스펙 13.4절 — bounded 풀 + CallerRunsPolicy)**

```kotlin
package com.seaotter.triggerly.adapter.persistence

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.ThreadPoolExecutor

@Configuration
@EnableAsync
class AsyncConfig {

  @Bean("memberSyncExecutor")
  fun memberSyncExecutor(
    @Value("\${triggerly.async.member-sync.core-pool-size:4}") corePoolSize: Int,
    @Value("\${triggerly.async.member-sync.max-pool-size:16}") maxPoolSize: Int,
    @Value("\${triggerly.async.member-sync.queue-capacity:1000}") queueCapacity: Int,
  ): ThreadPoolTaskExecutor {
    val executor = ThreadPoolTaskExecutor()
    executor.corePoolSize = corePoolSize
    executor.maxPoolSize = maxPoolSize
    executor.setQueueCapacity(queueCapacity)
    executor.setThreadNamePrefix("member-sync-")
    executor.setRejectedExecutionHandler(ThreadPoolExecutor.CallerRunsPolicy())
    executor.initialize()
    return executor
  }
}
```

- [ ] **Step 2: `ElasticsearchIntegrationTest.kt` (Testcontainers ES 베이스)**

```kotlin
package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@SpringBootTest(classes = [PersistenceTestApplication::class])
abstract class ElasticsearchIntegrationTest {
  companion object {
    @Container
    @ServiceConnection
    @JvmStatic
    val elasticsearch: ElasticsearchContainer =
      ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.15.0")
        .apply { withEnv("xpack.security.enabled", "false") }
  }
}
```

- [ ] **Step 3: 실패하는 테스트 작성 (`MemberEsSyncListenerTest`)**

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.ElasticsearchIntegrationTest
import com.seaotter.triggerly.adapter.persistence.MemberSavedEvent
import com.seaotter.triggerly.domain.Member
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import java.util.UUID
import kotlin.test.assertEquals

class MemberEsSyncListenerTest : ElasticsearchIntegrationTest() {

  @Autowired lateinit var eventPublisher: ApplicationEventPublisher
  @Autowired lateinit var repository: MemberElasticsearchRepository
  @Autowired lateinit var operations: ElasticsearchOperations

  @BeforeEach
  fun ensureIndex() {
    val indexOps = operations.indexOps(MemberDocument::class.java)
    if (!indexOps.exists()) indexOps.createWithMapping()
  }

  @Test
  fun `MemberSavedEvent가 발행되면 비동기로 ES에 반영된다`() {
    val member = Member(id = UUID.randomUUID().toString(), tenantId = "t1", externalMemberId = "ext-1", email = "a@b.com")

    eventPublisher.publishEvent(MemberSavedEvent(member))

    awaitAssert { assertEquals("a@b.com", repository.findById(member.id).orElseThrow().email) }
  }

  private fun <T> awaitAssert(timeoutMs: Long = 3000, intervalMs: Long = 100, block: () -> T): T {
    val deadline = System.currentTimeMillis() + timeoutMs
    var lastError: Throwable? = null
    while (System.currentTimeMillis() < deadline) {
      try {
        return block()
      } catch (e: Throwable) {
        lastError = e
        Thread.sleep(intervalMs)
      }
    }
    throw lastError ?: AssertionError("timeout")
  }
}
```

- [ ] **Step 4: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*MemberEsSyncListenerTest*"`
Expected: FAIL (컴파일 에러 — `MemberDocument`, `MemberElasticsearchRepository`, `MemberEsSyncListener` 없음)

- [ ] **Step 5: `MemberDocument`/`MemberElasticsearchRepository`/`MemberSearchAdapter`/`MemberEsSyncListener` 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import java.time.LocalDate

@Document(indexName = "member")
class MemberDocument(
  @Id var id: String,
  var tenantId: String,
  var externalMemberId: String,
  var name: String?,
  var email: String?,
  var telephone: String?,
  var devicePlatform: String?,
  var gender: String?,
  @Field(type = FieldType.Date, format = [org.springframework.data.elasticsearch.annotations.DateFormat.date])
  var birthday: LocalDate?,
  var status: String?,
  @Field(type = FieldType.Object)
  var attributes: Map<String, Any?>?,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository

interface MemberElasticsearchRepository : ElasticsearchRepository<MemberDocument, String>
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.application.port.MemberQueryPort
import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.Member
import com.seaotter.triggerly.domain.MemberStatus
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.stereotype.Component

@Component
class MemberSearchAdapter(private val operations: ElasticsearchOperations) : MemberQueryPort {

  override fun search(tenantId: String, criteria: Map<String, Any?>, limit: Int): List<Member> {
    var condition = Criteria("tenantId").`is`(tenantId)
    criteria.forEach { (field, value) -> condition = condition.and(Criteria(field).`is`(value)) }
    val query = CriteriaQuery(condition).setMaxResults(limit)
    return operations.search(query, MemberDocument::class.java).map { it.content.toDomain() }
  }

  private fun MemberDocument.toDomain() = Member(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.let { DevicePlatform.valueOf(it) },
    gender = gender?.let { Gender.valueOf(it) }, birthday = birthday,
    status = status?.let { MemberStatus.valueOf(it) }, attributes = attributes,
  )
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.MemberSavedEvent
import com.seaotter.triggerly.domain.Member
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class MemberEsSyncListener(private val repository: MemberElasticsearchRepository) {

  @Async("memberSyncExecutor")
  @EventListener
  fun onMemberSaved(event: MemberSavedEvent) {
    repository.save(event.member.toDocument())
  }

  private fun Member.toDocument() = MemberDocument(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.name, gender = gender?.name, birthday = birthday,
    status = status?.name, attributes = attributes,
  )
}
```

- [ ] **Step 6: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*MemberEsSyncListenerTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 7: `MemberSearchAdapterTest.kt` 작성 및 통과 확인**

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.ElasticsearchIntegrationTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import java.util.UUID
import kotlin.test.assertEquals

class MemberSearchAdapterTest : ElasticsearchIntegrationTest() {

  @Autowired lateinit var adapter: MemberSearchAdapter
  @Autowired lateinit var repository: MemberElasticsearchRepository
  @Autowired lateinit var operations: ElasticsearchOperations

  @BeforeEach
  fun setUp() {
    val indexOps = operations.indexOps(MemberDocument::class.java)
    if (!indexOps.exists()) indexOps.createWithMapping()
    repository.deleteAll()
  }

  @Test
  fun `search는 tenantId와 추가 조건을 AND로 결합해 검색한다`() {
    repository.save(MemberDocument(UUID.randomUUID().toString(), "t1", "ext-1", null, null, null, null, null, null, "ACTIVE", null))
    repository.save(MemberDocument(UUID.randomUUID().toString(), "t1", "ext-2", null, null, null, null, null, null, "WITHDRAWN", null))
    operations.indexOps(MemberDocument::class.java).refresh()

    val result = adapter.search("t1", mapOf("status" to "ACTIVE"))

    assertEquals(1, result.size)
    assertEquals("ext-1", result[0].externalMemberId)
  }
}
```

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*MemberSearchAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 8: Commit**

```bash
git add triggerly-adapter-out-persistence
git commit -m "feat(persistence): add Elasticsearch member sync (async, bounded executor) and search adapter"
```

---

### Task 12: EventInstance ES 색인 + MemberEventStatsPort

**Files:**
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/EventLogDocument.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/EventLogElasticsearchRepository.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/EventInstanceRepositoryAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/main/kotlin/com/seaotter/triggerly/adapter/persistence/es/MemberEventStatsAdapter.kt`
- Create: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/MySqlAndElasticsearchIntegrationTest.kt`
- Test: `triggerly-adapter-out-persistence/src/test/kotlin/com/seaotter/triggerly/adapter/persistence/es/EventInstanceRepositoryAdapterTest.kt`

**Interfaces:**
- Consumes: `EventInstanceRepositoryPort`, `MemberEventStatsPort` (Task 4), `EventInstanceMySqlStore`(Task 9)
- Produces: 두 포트의 완전한 구현 — Task 5(엔진)의 `stats.eventCount:*` 조건 평가, Task 16(컨슈머)의 이벤트 저장이 이걸 사용한다.
- **스펙 대비 단순화**: 스펙 13.4절은 ES 색인에 `BulkProcessor`/`BulkIngester`를 언급하지만, 데모 규모에서는 `ElasticsearchOperations.save()`(Spring Data가 관리하는 커넥션 풀 사용)로 단순화한다. 진짜 수만 rps를 실제로 검증하는 단계(스펙 14절, 범위 밖)에 가서 필요하면 `BulkIngester`로 교체하는 것을 후속 과제로 남긴다.

- [ ] **Step 1: `MySqlAndElasticsearchIntegrationTest.kt` (두 인프라를 모두 쓰는 테스트용 베이스)**

```kotlin
package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@SpringBootTest(classes = [PersistenceTestApplication::class])
abstract class MySqlAndElasticsearchIntegrationTest {
  companion object {
    @Container
    @ServiceConnection
    @JvmStatic
    val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4").apply { withDatabaseName("triggerly") }

    @Container
    @ServiceConnection
    @JvmStatic
    val elasticsearch: ElasticsearchContainer =
      ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.15.0")
        .apply { withEnv("xpack.security.enabled", "false") }
  }
}
```

- [ ] **Step 2: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.MySqlAndElasticsearchIntegrationTest
import com.seaotter.triggerly.domain.EventInstance
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class EventInstanceRepositoryAdapterTest : MySqlAndElasticsearchIntegrationTest() {

  @Autowired lateinit var adapter: EventInstanceRepositoryAdapter
  @Autowired lateinit var statsPort: MemberEventStatsAdapter
  @Autowired lateinit var operations: ElasticsearchOperations

  @BeforeEach
  fun setUp() {
    val indexOps = operations.indexOps(EventLogDocument::class.java)
    if (!indexOps.exists()) indexOps.createWithMapping()
  }

  @Test
  fun `save는 MySQL과 ES에 모두 저장되고, MemberEventStatsPort는 최근 N일 카운트를 센다`() {
    repeat(3) {
      adapter.save(
        EventInstance(
          id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "REVIEW_ADD",
          occurredAt = LocalDateTime.now(), memberId = "m1",
        ),
      )
    }
    adapter.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "REVIEW_ADD",
        occurredAt = LocalDateTime.now().minusDays(40), memberId = "m1",
      ),
    )
    operations.indexOps(EventLogDocument::class.java).refresh()

    val count = statsPort.countEvents("t1", "m1", "REVIEW_ADD", 30)

    assertEquals(3, count)
  }
}
```

- [ ] **Step 3: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*EventInstanceRepositoryAdapterTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 4: 구현**

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import java.time.LocalDateTime

@Document(indexName = "event_log")
class EventLogDocument(
  @Id var id: String,
  var tenantId: String,
  var eventCode: String,
  @Field(type = FieldType.Date) var occurredAt: LocalDateTime,
  var memberId: String?,
  @Field(type = FieldType.Object) var attributes: Map<String, Any?>?,
)
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository

interface EventLogElasticsearchRepository : ElasticsearchRepository<EventLogDocument, String>
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.jpa.EventInstanceMySqlStore
import com.seaotter.triggerly.application.port.EventInstanceRepositoryPort
import com.seaotter.triggerly.domain.EventInstance
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class EventInstanceRepositoryAdapter(
  private val mysqlStore: EventInstanceMySqlStore,
  private val esRepository: EventLogElasticsearchRepository,
) : EventInstanceRepositoryPort {

  override fun save(instance: EventInstance): EventInstance {
    val saved = mysqlStore.save(instance)
    esRepository.save(saved.toDocument())
    return saved
  }

  override fun deleteOlderThan(cutoff: LocalDateTime): Int = mysqlStore.deleteOlderThan(cutoff)

  private fun EventInstance.toDocument() = EventLogDocument(id, tenantId, eventCode, occurredAt, memberId, attributes)
}
```

```kotlin
package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.application.port.MemberEventStatsPort
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class MemberEventStatsAdapter(private val operations: ElasticsearchOperations) : MemberEventStatsPort {

  override fun countEvents(tenantId: String, memberId: String, eventCode: String, withinDays: Int): Long {
    val since = LocalDateTime.now().minusDays(withinDays.toLong())
    val condition = Criteria("tenantId").`is`(tenantId)
      .and(Criteria("memberId").`is`(memberId))
      .and(Criteria("eventCode").`is`(eventCode))
      .and(Criteria("occurredAt").greaterThanEqual(since))
    return operations.count(CriteriaQuery(condition), EventLogDocument::class.java)
  }
}
```

- [ ] **Step 5: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-out-persistence:test --tests "*EventInstanceRepositoryAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 6: persistence 모듈 전체 테스트**

Run: `./gradlew :triggerly-adapter-out-persistence:test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 7: Commit**

```bash
git add triggerly-adapter-out-persistence
git commit -m "feat(persistence): add event log ES indexing and MemberEventStatsPort"
```

---

### Task 13: Kafka 프로듀서

**Files:**
- Create: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/MessagingTestApplication.kt`
- Create: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/KafkaIntegrationTest.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/kafka/KafkaProducerConfig.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/kafka/KafkaEventPublisherAdapter.kt`
- Test: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/kafka/KafkaEventPublisherAdapterTest.kt`

**Interfaces:**
- Consumes: `EventPublisherPort`, `RawEventMessage` (Task 4)
- Produces: `const val RAW_EVENTS_TOPIC = "triggerly.events.raw"` — Task 16(컨슈머)이 같은 상수를 참조한다. `KafkaIntegrationTest`(Testcontainers Kafka 베이스) — Task 16 테스트가 상속.

- [ ] **Step 1: 테스트 인프라**

```kotlin
package com.seaotter.triggerly.adapter.messaging

import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class MessagingTestApplication
```

```kotlin
package com.seaotter.triggerly.adapter.messaging

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer

@Testcontainers
@SpringBootTest(classes = [MessagingTestApplication::class])
abstract class KafkaIntegrationTest {
  companion object {
    @Container
    @ServiceConnection
    @JvmStatic
    val kafka: KafkaContainer = KafkaContainer("apache/kafka:3.7.0")
  }
}
```

- [ ] **Step 2: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaEventPublisherAdapterTest : KafkaIntegrationTest() {

  @Autowired lateinit var adapter: KafkaEventPublisherAdapter

  @Test
  fun `publish는 tenantId콜론externalMemberId를 키로 raw-events 토픽에 produce한다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "PURCHASE", externalMemberId = "ext-1",
      memberContext = null, attributes = mapOf("amount" to 1000), occurredAt = LocalDateTime.now(),
    )
    adapter.publish(message)

    val props = Properties().apply {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIntegrationTest.kafka.bootstrapServers)
      put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-${System.nanoTime()}")
      put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
    }
    KafkaConsumer<String, String>(props).use { consumer ->
      consumer.subscribe(listOf(RAW_EVENTS_TOPIC))
      val records = consumer.poll(Duration.ofSeconds(10))
      assertEquals(1, records.count())
      val record = records.first()
      assertEquals("t1:ext-1", record.key())
      assertTrue(record.value().contains("PURCHASE"))
    }
  }
}
```

- [ ] **Step 3: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*KafkaEventPublisherAdapterTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 4: 구현**

```kotlin
package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

const val RAW_EVENTS_TOPIC = "triggerly.events.raw"

@Configuration
class KafkaProducerConfig(private val kafkaProperties: KafkaProperties) {

  @Bean
  fun rawEventsTopic(
    @Value("\${triggerly.kafka.raw-events-topic.partitions:32}") partitions: Int,
  ): NewTopic = TopicBuilder.name(RAW_EVENTS_TOPIC).partitions(partitions).replicas(1).build()

  @Bean
  fun rawEventProducerFactory(): ProducerFactory<String, RawEventMessage> {
    val props = kafkaProperties.buildProducerProperties(null)
    return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer())
  }

  @Bean
  fun rawEventKafkaTemplate(producerFactory: ProducerFactory<String, RawEventMessage>): KafkaTemplate<String, RawEventMessage> =
    KafkaTemplate(producerFactory)
}
```

```kotlin
package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RawEventMessage
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaEventPublisherAdapter(
  private val kafkaTemplate: KafkaTemplate<String, RawEventMessage>,
) : EventPublisherPort {

  private val log = LoggerFactory.getLogger(KafkaEventPublisherAdapter::class.java)

  // 13.1절: 컨트롤러 스레드는 produce 완료를 기다리지 않는다 — 실패는 로깅만 하고 요청 흐름을 막지 않는다.
  override fun publish(message: RawEventMessage) {
    val key = "${message.tenantId}:${message.externalMemberId ?: "anon"}"
    kafkaTemplate.send(RAW_EVENTS_TOPIC, key, message).whenComplete { _, ex ->
      if (ex != null) log.error("Kafka publish 실패: key=$key eventCode=${message.eventCode}", ex)
    }
  }
}
```

- [ ] **Step 5: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*KafkaEventPublisherAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 6: Commit**

```bash
git add triggerly-adapter-messaging
git commit -m "feat(messaging): add Kafka raw-events producer (partitioned by tenantId:externalMemberId)"
```

---

### Task 14: Redis 대기 인덱스

**Files:**
- Create: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/RedisIntegrationTest.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisWaitingIndexAdapter.kt`
- Test: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisWaitingIndexAdapterTest.kt`

**Interfaces:**
- Consumes: `WaitingIndexPort` (Task 4)
- Produces: `WaitingIndexPort`의 완전한 구현 — Task 5(엔진), Task 6(유스케이스), Task 16(컨슈머)이 이미 이 포트를 소비 중. `RedisIntegrationTest`(Testcontainers Redis 베이스) — Task 15가 상속.

- [ ] **Step 1: `RedisIntegrationTest.kt` (Testcontainers Redis는 공식 Spring Boot `@ServiceConnection` 대상이 아니므로 `GenericContainer` + `@DynamicPropertySource` 사용)**

```kotlin
package com.seaotter.triggerly.adapter.messaging

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@Testcontainers
@SpringBootTest(classes = [MessagingTestApplication::class])
abstract class RedisIntegrationTest {
  companion object {
    @Container
    @JvmStatic
    val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379)

    @JvmStatic
    @DynamicPropertySource
    fun registerRedisProperties(registry: DynamicPropertyRegistry) {
      registry.add("spring.data.redis.host") { redis.host }
      registry.add("spring.data.redis.port") { redis.getMappedPort(6379) }
    }
  }
}
```

- [ ] **Step 2: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RedisWaitingIndexAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisWaitingIndexAdapter

  @Test
  fun `register한 인스턴스를 lookup으로 찾고, remove하면 사라진다`() {
    adapter.register("t1", "PURCHASE", "m1", "instance-1")
    adapter.register("t1", "PURCHASE", "m1", "instance-2")

    assertEquals(listOf("instance-1", "instance-2"), adapter.lookup("t1", "PURCHASE", "m1"))

    adapter.remove("t1", "PURCHASE", "m1", "instance-1")

    assertEquals(listOf("instance-2"), adapter.lookup("t1", "PURCHASE", "m1"))
    assertTrue(adapter.lookup("t1", "OTHER_EVENT", "m1").isEmpty())
  }
}
```

- [ ] **Step 3: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisWaitingIndexAdapterTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 4: 구현**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.WaitingIndexPort
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisWaitingIndexAdapter(
  private val redisTemplate: StringRedisTemplate,
) : WaitingIndexPort {

  private fun key(tenantId: String, eventCode: String, memberId: String) =
    "waiting:$tenantId:event:$eventCode:member:$memberId"

  override fun register(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String) {
    redisTemplate.opsForList().rightPush(key(tenantId, eventCode, memberId), workflowInstanceId)
  }

  override fun remove(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String) {
    redisTemplate.opsForList().remove(key(tenantId, eventCode, memberId), 0, workflowInstanceId)
  }

  override fun lookup(tenantId: String, eventCode: String, memberId: String): List<String> =
    redisTemplate.opsForList().range(key(tenantId, eventCode, memberId), 0, -1) ?: emptyList()
}
```

- [ ] **Step 5: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisWaitingIndexAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 6: Commit**

```bash
git add triggerly-adapter-messaging
git commit -m "feat(messaging): add Redis waiting index adapter for WAIT_FOR_EVENT matching"
```

---

### Task 15: Redis 테넌트 레이트리밋

**Files:**
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RateLimitScript.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisRateLimiterAdapter.kt`
- Test: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/redis/RedisRateLimiterAdapterTest.kt`

**Interfaces:**
- Consumes: `RateLimiterPort` (Task 4), `RedisIntegrationTest`(Task 14)
- Produces: `RateLimiterPort`의 완전한 구현 — Task 17(이벤트 수집 API)이 그대로 소비한다.
- **설계 결정**: 스펙 13.2절은 "토큰 버킷(Lua 스크립트 또는 Bucket4j-Redis)"이라고 both 언급하지만, 별도 라이브러리 의존성/버전 리스크를 줄이기 위해 Lua 기반 **고정 윈도우 카운터**로 구체화한다 — `INCR` 후 최초 호출에만 `EXPIRE`를 걸어 초당 요청 수를 센다. 토큰 버킷보다 단순하지만 "테넌트별 초당 요청 수 제한"이라는 목표는 동일하게 달성한다.

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestPropertySource(properties = ["triggerly.ratelimit.default-rps=3"])
class RedisRateLimiterAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisRateLimiterAdapter

  @Test
  fun `설정된 rps를 초과하면 tryConsume이 false를 반환한다`() {
    val tenantId = "t-ratelimit-${System.nanoTime()}"
    assertTrue(adapter.tryConsume(tenantId))
    assertTrue(adapter.tryConsume(tenantId))
    assertTrue(adapter.tryConsume(tenantId))
    assertFalse(adapter.tryConsume(tenantId))
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisRateLimiterAdapterTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import org.springframework.data.redis.core.script.DefaultRedisScript

object RateLimitScript {
  private const val SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
  return 0
else
  return 1
end
"""

  val INSTANCE: DefaultRedisScript<Long> = DefaultRedisScript(SCRIPT, Long::class.java)
}
```

```kotlin
package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.RateLimiterPort
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class RedisRateLimiterAdapter(
  private val redisTemplate: StringRedisTemplate,
  @Value("\${triggerly.ratelimit.default-rps:2000}") private val defaultRps: Int,
) : RateLimiterPort {

  override fun tryConsume(tenantId: String): Boolean {
    val windowSecond = Instant.now().epochSecond
    val key = "ratelimit:$tenantId:$windowSecond"
    // ARGV[2]=2초 TTL — 윈도우가 끝난 직후 들어오는 클럭 오차를 흡수하기 위한 여유
    val result = redisTemplate.execute(RateLimitScript.INSTANCE, listOf(key), defaultRps.toString(), "2")
    return result == 1L
  }
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*RedisRateLimiterAdapterTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-adapter-messaging
git commit -m "feat(messaging): add Redis fixed-window per-tenant rate limiter"
```

---

### Task 16: Kafka 컨슈머 (WorkflowTriggerConsumer)

**Files:**
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/kafka/KafkaConsumerConfig.kt`
- Create: `triggerly-adapter-messaging/src/main/kotlin/com/seaotter/triggerly/adapter/messaging/kafka/WorkflowTriggerConsumer.kt`
- Test: `triggerly-adapter-messaging/src/test/kotlin/com/seaotter/triggerly/adapter/messaging/kafka/WorkflowTriggerConsumerTest.kt`

**Interfaces:**
- Consumes: `IngestEventUseCase.handle(RawEventMessage)` (Task 6), `RAW_EVENTS_TOPIC`(Task 13)
- Produces: 스펙 6절의 컨슈머 로직 완성. 이걸로 messaging 모듈 완성 — 이후 Task 17~20(web)과 Task 21(bootstrap)이 전체를 조립한다.
- **설계 결정**: 배치 리스너(`isBatchListener = true`)로 `max.poll.records`(기본 500)만큼 한 번에 가져와 `IngestEventUseCase.handle()`을 레코드별로 호출한다 — 13.3절의 "배치 컨슈밍으로 메모리 상한을 둔다"는 목표를 이렇게 구현한다. `IngestEventUseCase` 자체는 이벤트마다 다른 워크플로를 매칭시켜야 하므로 레코드 단위로 처리한다(진짜 벌크 저장 최적화는 12절에서 이미 MySQL/ES 어댑터 내부로 캡슐화됨).

- [ ] **Step 1: 실패하는 테스트 작성 (Testcontainers Kafka + IngestEventUseCase는 mockk)**

```kotlin
package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import java.time.Duration

class WorkflowTriggerConsumerTest : KafkaIntegrationTest() {

  @Autowired lateinit var adapter: KafkaEventPublisherAdapter

  @org.springframework.boot.test.context.TestConfiguration
  class MockConfig {
    @org.springframework.context.annotation.Bean
    fun ingestEventUseCase(): IngestEventUseCase = io.mockk.mockk(relaxed = true)
  }

  @Autowired lateinit var ingestEventUseCase: IngestEventUseCase

  @Test
  fun `raw-events 토픽에 produce된 메시지는 IngestEventUseCase handle로 전달된다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
    )
    adapter.publish(message)

    Thread.sleep(Duration.ofSeconds(5).toMillis())

    verify(timeout = 5000) { ingestEventUseCase.handle(match { it.eventCode == "LOGIN" && it.externalMemberId == "ext-1" }) }
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*WorkflowTriggerConsumerTest*"`
Expected: FAIL (컴파일 에러 — `WorkflowTriggerConsumer` 없음)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
class KafkaConsumerConfig(private val kafkaProperties: KafkaProperties) {

  @Bean
  fun rawEventConsumerFactory(
    @Value("\${triggerly.kafka.consumer.max-poll-records:500}") maxPollRecords: Int,
  ): ConsumerFactory<String, RawEventMessage> {
    val props = kafkaProperties.buildConsumerProperties(null).toMutableMap()
    props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
    props[ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS] = StringDeserializer::class.java
    props[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = JsonDeserializer::class.java
    props[JsonDeserializer.TRUSTED_PACKAGES] = "com.seaotter.triggerly.application.port"
    props[JsonDeserializer.VALUE_DEFAULT_TYPE] = RawEventMessage::class.java.name
    return DefaultKafkaConsumerFactory(props)
  }

  @Bean
  fun rawEventBatchListenerContainerFactory(
    consumerFactory: ConsumerFactory<String, RawEventMessage>,
  ): ConcurrentKafkaListenerContainerFactory<String, RawEventMessage> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, RawEventMessage>()
    factory.consumerFactory = consumerFactory
    factory.isBatchListener = true
    return factory
  }
}
```

```kotlin
package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class WorkflowTriggerConsumer(private val ingestEventUseCase: IngestEventUseCase) {

  private val log = LoggerFactory.getLogger(WorkflowTriggerConsumer::class.java)

  @KafkaListener(
    topics = [RAW_EVENTS_TOPIC],
    groupId = "triggerly-engine",
    concurrency = "\${triggerly.kafka.consumer.concurrency:8}",
    containerFactory = "rawEventBatchListenerContainerFactory",
  )
  fun onMessages(records: List<ConsumerRecord<String, RawEventMessage>>) {
    records.forEach { record ->
      runCatching { ingestEventUseCase.handle(record.value()) }
        .onFailure { log.error("이벤트 처리 실패: key=${record.key()}", it) }
    }
  }
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-messaging:test --tests "*WorkflowTriggerConsumerTest*"`
Expected: `BUILD SUCCESSFUL`, 1 test passed

- [ ] **Step 5: messaging 모듈 전체 테스트**

Run: `./gradlew :triggerly-adapter-messaging:test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: Commit**

```bash
git add triggerly-adapter-messaging
git commit -m "feat(messaging): add WorkflowTriggerConsumer wiring Kafka to IngestEventUseCase"
```

---

### Task 17: 이벤트 수집 REST API

**Files:**
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/dto/EventRequestDto.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/controller/EventIngestionController.kt`
- Test: `triggerly-adapter-in-web/src/test/kotlin/com/seaotter/triggerly/adapter/web/controller/EventIngestionControllerTest.kt`

**Interfaces:**
- Consumes: `EventDefinitionPort`, `RateLimiterPort`, `EventPublisherPort`, `RawEventMessage` (Task 4)
- Produces: `POST /api/v1/events` — Task 23(SDK)이 호출하는 실제 엔드포인트.
- **테스트 방침**: 컨트롤러가 얇으므로(포트 3개 조합) `@WebMvcTest`/`MockMvc` 없이 컨트롤러를 mockk로 직접 생성해 순수 단위 테스트로 검증한다. 실제 HTTP 왕복 및 JSON 역직렬화는 Task 24 통합 테스트에서 검증한다. 가상 스레드(`spring.threads.virtual.enabled`)와 요청 바디 크기 제한은 코드가 아니라 Task 21의 `application.yml` 설정이다(스펙 13.1절).

- [ ] **Step 1: DTO 작성**

```kotlin
package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.MemberContext
import java.time.LocalDate

data class EventRequestDto(
  val tenantId: String,
  val eventCode: String,
  val member: MemberContextDto? = null,
  val attributes: Map<String, Any?>? = null,
)

data class MemberContextDto(
  val externalMemberId: String,
  val name: String? = null,
  val email: String? = null,
  val telephone: String? = null,
  val devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  val birthday: LocalDate? = null,
) {
  fun toDomain(tenantId: String) = MemberContext(
    tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform, gender = gender, birthday = birthday,
  )
}
```

- [ ] **Step 2: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.EventRequestDto
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RateLimiterPort
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.springframework.http.HttpStatus
import kotlin.test.Test
import kotlin.test.assertEquals

class EventIngestionControllerTest {

  private val eventDefinitionPort = mockk<EventDefinitionPort>()
  private val rateLimiterPort = mockk<RateLimiterPort>()
  private val eventPublisherPort = mockk<EventPublisherPort>(relaxed = true)
  private val controller = EventIngestionController(eventDefinitionPort, rateLimiterPort, eventPublisherPort)

  @Test
  fun `등록된 eventCode고 레이트리밋을 통과하면 202를 반환하고 Kafka로 publish한다`() {
    every { rateLimiterPort.tryConsume("t1") } returns true
    every { eventDefinitionPort.findByTenantAndCode("t1", "PURCHASE") } returns EventDefinition("t1", "PURCHASE", "구매")

    val response = controller.ingest(EventRequestDto(tenantId = "t1", eventCode = "PURCHASE"))

    assertEquals(HttpStatus.ACCEPTED, response.statusCode)
    verify { eventPublisherPort.publish(match { it.tenantId == "t1" && it.eventCode == "PURCHASE" }) }
  }

  @Test
  fun `레이트리밋을 초과하면 429를 반환하고 publish하지 않는다`() {
    every { rateLimiterPort.tryConsume("t1") } returns false

    val response = controller.ingest(EventRequestDto(tenantId = "t1", eventCode = "PURCHASE"))

    assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.statusCode)
    verify(exactly = 0) { eventPublisherPort.publish(any()) }
  }

  @Test
  fun `등록되지 않은 eventCode는 400을 반환한다`() {
    every { rateLimiterPort.tryConsume("t1") } returns true
    every { eventDefinitionPort.findByTenantAndCode("t1", "UNKNOWN") } returns null

    val response = controller.ingest(EventRequestDto(tenantId = "t1", eventCode = "UNKNOWN"))

    assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
    verify(exactly = 0) { eventPublisherPort.publish(any()) }
  }
}
```

- [ ] **Step 3: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*EventIngestionControllerTest*"`
Expected: FAIL (컴파일 에러 — `EventIngestionController` 없음)

- [ ] **Step 4: 구현**

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.EventRequestDto
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RateLimiterPort
import com.seaotter.triggerly.application.port.RawEventMessage
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.time.LocalDateTime

@RestController
class EventIngestionController(
  private val eventDefinitionPort: EventDefinitionPort,
  private val rateLimiterPort: RateLimiterPort,
  private val eventPublisherPort: EventPublisherPort,
) {

  @PostMapping("/api/v1/events")
  fun ingest(@RequestBody request: EventRequestDto): ResponseEntity<Any> {
    if (!rateLimiterPort.tryConsume(request.tenantId)) {
      return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).header(HttpHeaders.RETRY_AFTER, "1").build()
    }
    if (eventDefinitionPort.findByTenantAndCode(request.tenantId, request.eventCode) == null) {
      return ResponseEntity.badRequest().body(mapOf("error" to "unknown eventCode: ${request.eventCode}"))
    }

    eventPublisherPort.publish(
      RawEventMessage(
        tenantId = request.tenantId,
        eventCode = request.eventCode,
        externalMemberId = request.member?.externalMemberId,
        memberContext = request.member?.toDomain(request.tenantId),
        attributes = request.attributes,
        occurredAt = LocalDateTime.now(),
      ),
    )
    return ResponseEntity.accepted().build()
  }
}
```

- [ ] **Step 5: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*EventIngestionControllerTest*"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed

- [ ] **Step 6: Commit**

```bash
git add triggerly-adapter-in-web
git commit -m "feat(web): add event ingestion API with rate-limit and event-definition validation"
```

---

### Task 18: 어드민 EventDefinition/AttributeDefinition REST API

**Files:**
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/dto/EventDefinitionDtos.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/controller/EventDefinitionController.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/dto/AttributeDefinitionDtos.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/controller/AttributeDefinitionController.kt`
- Test: `triggerly-adapter-in-web/src/test/kotlin/com/seaotter/triggerly/adapter/web/controller/EventDefinitionControllerTest.kt`
- Test: `triggerly-adapter-in-web/src/test/kotlin/com/seaotter/triggerly/adapter/web/controller/AttributeDefinitionControllerTest.kt`

**Interfaces:**
- Consumes: `ManageEventDefinitionUseCase`, `ManageAttributeDefinitionUseCase` (Task 6)
- Produces: `POST/GET /api/v1/admin/event-definitions`, `POST/GET /api/v1/admin/attribute-definitions` — Task 22(DemoRunner)와 Task 26(demo.sh)이 시드 데이터 등록에 사용한다.

- [ ] **Step 1: 실패하는 테스트 2개 작성**

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterEventDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class EventDefinitionControllerTest {

  private val useCase = mockk<ManageEventDefinitionUseCase>()
  private val controller = EventDefinitionController(useCase)

  @Test
  fun `register는 유스케이스에 위임한다`() {
    every { useCase.register("t1", "PURCHASE", "구매") } returns EventDefinition("t1", "PURCHASE", "구매")

    val result = controller.register(RegisterEventDefinitionRequest("t1", "PURCHASE", "구매"))

    assertEquals("PURCHASE", result.code)
  }

  @Test
  fun `list는 테넌트의 전체 목록을 반환한다`() {
    every { useCase.list("t1") } returns listOf(EventDefinition("t1", "PURCHASE", "구매"))

    assertEquals(1, controller.list("t1").size)
  }
}
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterAttributeDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageAttributeDefinitionUseCase
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import io.mockk.every
import io.mockk.mockk
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class AttributeDefinitionControllerTest {

  private val useCase = mockk<ManageAttributeDefinitionUseCase>()
  private val controller = AttributeDefinitionController(useCase)

  @Test
  fun `register는 유스케이스에 위임한다`() {
    val expected = AttributeDefinition(UUID.randomUUID().toString(), "t1", null, "amount", "금액", AttributeType.LONG, true)
    every { useCase.register("t1", null, "amount", "금액", AttributeType.LONG, true) } returns expected

    val result = controller.register(RegisterAttributeDefinitionRequest("t1", null, "amount", "금액", AttributeType.LONG, true))

    assertEquals("amount", result.key)
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*EventDefinitionControllerTest*" --tests "*AttributeDefinitionControllerTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.web.dto

data class RegisterEventDefinitionRequest(val tenantId: String, val code: String, val displayName: String)
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterEventDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/event-definitions")
class EventDefinitionController(private val useCase: ManageEventDefinitionUseCase) {

  @PostMapping
  fun register(@RequestBody request: RegisterEventDefinitionRequest): EventDefinition =
    useCase.register(request.tenantId, request.code, request.displayName)

  @GetMapping
  fun list(@RequestParam tenantId: String): List<EventDefinition> = useCase.list(tenantId)
}
```

```kotlin
package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.AttributeType

data class RegisterAttributeDefinitionRequest(
  val tenantId: String,
  val eventDefinitionId: String?,
  val key: String,
  val displayName: String,
  val type: AttributeType,
  val filterable: Boolean = false,
)
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterAttributeDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageAttributeDefinitionUseCase
import com.seaotter.triggerly.domain.AttributeDefinition
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/attribute-definitions")
class AttributeDefinitionController(private val useCase: ManageAttributeDefinitionUseCase) {

  @PostMapping
  fun register(@RequestBody request: RegisterAttributeDefinitionRequest): AttributeDefinition =
    useCase.register(request.tenantId, request.eventDefinitionId, request.key, request.displayName, request.type, request.filterable)

  @GetMapping
  fun list(@RequestParam tenantId: String): List<AttributeDefinition> = useCase.list(tenantId)
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*EventDefinitionControllerTest*" --tests "*AttributeDefinitionControllerTest*"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-adapter-in-web
git commit -m "feat(web): add admin EventDefinition/AttributeDefinition APIs"
```

---

### Task 19: 어드민 Workflow REST API

**Files:**
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/dto/WorkflowDtos.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/controller/WorkflowController.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/controller/WorkflowInstanceController.kt`
- Test: `triggerly-adapter-in-web/src/test/kotlin/com/seaotter/triggerly/adapter/web/controller/WorkflowControllerTest.kt`
- Test: `triggerly-adapter-in-web/src/test/kotlin/com/seaotter/triggerly/adapter/web/controller/WorkflowInstanceControllerTest.kt`

**Interfaces:**
- Consumes: `ManageWorkflowUseCase`, `WorkflowInstanceQueryUseCase`(+`WorkflowInstanceView`) (Task 6)
- Produces: `POST/GET/PUT /api/v1/admin/workflows`, `POST /api/v1/admin/workflows/{id}/enable`, `GET /api/v1/admin/workflow-instances/{id}` — Task 22/26이 워크플로 등록·활성화에, Task 24가 진행 상태 확인에 사용한다.

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.CreateWorkflowRequest
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.domain.*
import io.mockk.every
import io.mockk.mockk
import org.springframework.http.HttpStatus
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkflowControllerTest {

  private val useCase = mockk<ManageWorkflowUseCase>()
  private val controller = WorkflowController(useCase)

  private fun sample() = Workflow(
    id = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
    definitionJson = WorkflowDefinition("LOGIN", listOf(Node.Trigger("n1", "LOGIN"), Node.End("n2")), listOf(Edge("n1", "n2", EdgeRoute.Always))),
    status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )

  @Test
  fun `create는 DRAFT 워크플로를 생성한다`() {
    every { useCase.create(any()) } returns sample()

    val result = controller.create(
      CreateWorkflowRequest("wf-1", "t1", null, "LOGIN", sample().definitionJson),
    )

    assertEquals("wf-1", result.id)
  }

  @Test
  fun `get은 없는 워크플로면 404를 반환한다`() {
    every { useCase.get("t1", "unknown") } returns null

    val response = controller.get("t1", "unknown")

    assertEquals(HttpStatus.NOT_FOUND, response.statusCode)
  }

  @Test
  fun `enable은 유스케이스에 위임한다`() {
    every { useCase.enable("t1", "wf-1") } returns sample().apply { status = WorkflowStatus.ENABLED }

    val result = controller.enable("t1", "wf-1")

    assertEquals(WorkflowStatus.ENABLED, result.status)
  }
}
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.application.usecase.WorkflowInstanceQueryUseCase
import com.seaotter.triggerly.application.usecase.WorkflowInstanceView
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import org.springframework.http.HttpStatus
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkflowInstanceControllerTest {

  private val useCase = mockk<WorkflowInstanceQueryUseCase>()
  private val controller = WorkflowInstanceController(useCase)

  @Test
  fun `존재하는 인스턴스는 200과 함께 반환한다`() {
    val instance = WorkflowInstance(
      id = "i1", workflowId = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
      version = 1, status = WorkflowInstanceStatus.COMPLETED,
    )
    every { useCase.get("i1") } returns WorkflowInstanceView(instance, emptyList())

    val response = controller.get("i1")

    assertEquals(HttpStatus.OK, response.statusCode)
  }

  @Test
  fun `없는 인스턴스는 404를 반환한다`() {
    every { useCase.get("unknown") } returns null

    assertEquals(HttpStatus.NOT_FOUND, controller.get("unknown").statusCode)
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*WorkflowControllerTest*" --tests "*WorkflowInstanceControllerTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import java.time.LocalDateTime

data class CreateWorkflowRequest(
  val id: String,
  val tenantId: String,
  val name: String?,
  val triggerEventCode: String,
  val definitionJson: WorkflowDefinition,
) {
  fun toDomain() = Workflow(
    id = id, tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
    definitionJson = definitionJson, status = WorkflowStatus.DRAFT,
    createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )
}
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.CreateWorkflowRequest
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.domain.Workflow
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/workflows")
class WorkflowController(private val useCase: ManageWorkflowUseCase) {

  @PostMapping
  fun create(@RequestBody request: CreateWorkflowRequest): Workflow = useCase.create(request.toDomain())

  @GetMapping
  fun list(@RequestParam tenantId: String): List<Workflow> = useCase.list(tenantId)

  @GetMapping("/{id}")
  fun get(@RequestParam tenantId: String, @PathVariable id: String): ResponseEntity<Workflow> =
    useCase.get(tenantId, id)?.let { ResponseEntity.ok(it) } ?: ResponseEntity.notFound().build()

  @PostMapping("/{id}/enable")
  fun enable(@RequestParam tenantId: String, @PathVariable id: String): Workflow = useCase.enable(tenantId, id)
}
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.application.usecase.WorkflowInstanceQueryUseCase
import com.seaotter.triggerly.application.usecase.WorkflowInstanceView
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/admin/workflow-instances")
class WorkflowInstanceController(private val useCase: WorkflowInstanceQueryUseCase) {

  @GetMapping("/{id}")
  fun get(@PathVariable id: String): ResponseEntity<WorkflowInstanceView> =
    useCase.get(id)?.let { ResponseEntity.ok(it) } ?: ResponseEntity.notFound().build()
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*WorkflowControllerTest*" --tests "*WorkflowInstanceControllerTest*"`
Expected: `BUILD SUCCESSFUL`, 5 tests passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-adapter-in-web
git commit -m "feat(web): add admin Workflow CRUD/enable and WorkflowInstance query APIs"
```

---

### Task 20: 어드민 Member REST API + OpenAPI

**Files:**
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/dto/MemberDtos.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/controller/MemberController.kt`
- Create: `triggerly-adapter-in-web/src/main/kotlin/com/seaotter/triggerly/adapter/web/OpenApiConfig.kt`
- Test: `triggerly-adapter-in-web/src/test/kotlin/com/seaotter/triggerly/adapter/web/controller/MemberControllerTest.kt`

**Interfaces:**
- Consumes: `ManageMemberUseCase` (Task 6)
- Produces: `POST /api/v1/admin/members`, `GET /api/v1/admin/members/search` — Task 22/26이 사용. `springdoc`이 `/v3/api-docs`, `/swagger-ui.html`을 자동 노출.

- [ ] **Step 1: 실패하는 테스트 작성**

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterMemberRequest
import com.seaotter.triggerly.application.usecase.ManageMemberUseCase
import com.seaotter.triggerly.domain.Member
import io.mockk.every
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class MemberControllerTest {

  private val useCase = mockk<ManageMemberUseCase>()
  private val controller = MemberController(useCase)

  @Test
  fun `register는 유스케이스에 위임한다`() {
    every { useCase.register(any()) } answers { firstArg() }

    val result = controller.register(RegisterMemberRequest("m1", "t1", "ext-1", email = "a@b.com"))

    assertEquals("a@b.com", result.email)
  }

  @Test
  fun `search는 tenantId를 제외한 나머지 파라미터를 조건으로 넘긴다`() {
    every { useCase.search("t1", mapOf("status" to "ACTIVE")) } returns listOf(
      Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1"),
    )

    val result = controller.search("t1", mapOf("tenantId" to "t1", "status" to "ACTIVE"))

    assertEquals(1, result.size)
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*MemberControllerTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.Member
import java.time.LocalDate

data class RegisterMemberRequest(
  val id: String,
  val tenantId: String,
  val externalMemberId: String,
  val name: String? = null,
  val email: String? = null,
  val telephone: String? = null,
  val devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  val birthday: LocalDate? = null,
  val attributes: Map<String, Any?>? = null,
) {
  fun toDomain() = Member(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform, gender = gender, birthday = birthday,
    attributes = attributes,
  )
}
```

```kotlin
package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterMemberRequest
import com.seaotter.triggerly.application.usecase.ManageMemberUseCase
import com.seaotter.triggerly.domain.Member
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/members")
class MemberController(private val useCase: ManageMemberUseCase) {

  @PostMapping
  fun register(@RequestBody request: RegisterMemberRequest): Member = useCase.register(request.toDomain())

  @GetMapping("/search")
  fun search(@RequestParam tenantId: String, @RequestParam allParams: Map<String, String>): List<Member> =
    useCase.search(tenantId, allParams - "tenantId")
}
```

```kotlin
package com.seaotter.triggerly.adapter.web

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {
  @Bean
  fun triggerlyOpenApi(): OpenAPI = OpenAPI().info(Info().title("triggerly-claude API").version("v1"))
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-adapter-in-web:test --tests "*MemberControllerTest*"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: web 모듈 전체 테스트**

Run: `./gradlew :triggerly-adapter-in-web:test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: Commit**

```bash
git add triggerly-adapter-in-web
git commit -m "feat(web): add admin Member API and OpenAPI documentation"
```

---

### Task 21: 부트스트랩 (메인/설정/스케줄러/Actuator)

**Files:**
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/TriggerlyClaudeApplication.kt`
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/WaitingInstanceTimeoutPoller.kt`
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/EventInstanceCleanupJob.kt`
- Create: `triggerly-bootstrap/src/main/resources/application.yml`
- Create: `triggerly-bootstrap/src/main/resources/application-local.yml`
- Test: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/WaitingInstanceTimeoutPollerTest.kt`
- Test: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/EventInstanceCleanupJobTest.kt`

**Interfaces:**
- Consumes: 7개 모듈 전체 (도메인 제외 모든 포트/유스케이스/어댑터)
- Produces: 실행 가능한 Spring Boot 앱. `WaitingInstanceTimeoutPoller`/`EventInstanceCleanupJob`은 스펙 6절 4단계, 5.1절의 30일 보관 정책을 구현한다.

- [ ] **Step 1: 메인 애플리케이션 클래스**

```kotlin
package com.seaotter.triggerly.bootstrap

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication(scanBasePackages = ["com.seaotter.triggerly"])
@EnableScheduling
class TriggerlyClaudeApplication

fun main(args: Array<String>) {
  runApplication<TriggerlyClaudeApplication>(*args)
}
```

- [ ] **Step 2: `application.yml` (공통 기본값 — 13절 대용량 트래픽 설정값 전부 여기서 노출)**

```yaml
spring:
  application:
    name: triggerly-claude
  threads:
    virtual:
      enabled: true
  flyway:
    locations: classpath:db/migration
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false

server:
  tomcat:
    max-swallow-size: 2MB

triggerly:
  kafka:
    raw-events-topic:
      partitions: 32
    consumer:
      concurrency: 8
      max-poll-records: 500
  ratelimit:
    default-rps: 2000
  async:
    member-sync:
      core-pool-size: 4
      max-pool-size: 16
      queue-capacity: 1000
  scheduler:
    timeout-poll-interval-ms: 10000
    event-instance-retention-days: 30

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,metrics
```

- [ ] **Step 3: `application-local.yml` (docker-compose 기본 포트)**

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/triggerly
    username: triggerly
    password: triggerly
  elasticsearch:
    uris: http://localhost:9200
  data:
    redis:
      host: localhost
      port: 6379
  kafka:
    bootstrap-servers: localhost:9092
```

- [ ] **Step 4: 실패하는 테스트 2개 작성 (mockk, Spring 컨텍스트 없이 순수 단위 테스트)**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class WaitingInstanceTimeoutPollerTest {

  @Test
  fun `만료된 WAITING 인스턴스마다 타임아웃 합성 이벤트를 publish한다`() {
    val instancePort = mockk<WorkflowInstanceRepositoryPort>()
    val publisherPort = mockk<EventPublisherPort>(relaxed = true)
    val expired = WorkflowInstance(
      id = "i1", workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
      version = 1, status = WorkflowInstanceStatus.WAITING,
    )
    every { instancePort.findWaitingExpired(any()) } returns listOf(expired)

    WaitingInstanceTimeoutPoller(instancePort, publisherPort).pollExpiredInstances()

    verify { publisherPort.publish(match { it.syntheticTimeoutForInstanceId == "i1" && it.tenantId == "t1" }) }
  }
}
```

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventInstanceRepositoryPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class EventInstanceCleanupJobTest {

  @Test
  fun `retentionDays 이전 데이터를 삭제한다`() {
    val port = mockk<EventInstanceRepositoryPort>()
    every { port.deleteOlderThan(any()) } returns 5

    EventInstanceCleanupJob(port, retentionDays = 30).cleanup()

    verify { port.deleteOlderThan(any()) }
  }
}
```

- [ ] **Step 5: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-bootstrap:test --tests "*WaitingInstanceTimeoutPollerTest*" --tests "*EventInstanceCleanupJobTest*"`
Expected: FAIL (컴파일 에러)

- [ ] **Step 6: 구현**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

// 스펙 6절 4단계: WAITING인데 waitingUntil이 지난 인스턴스를 찾아 타임아웃 합성 이벤트를
// 같은 Kafka 토픽에 다시 produce한다 — 엔진이 실제 노드 전이는 컨슈머 경로 하나로만 처리하게 하기 위함.
@Component
class WaitingInstanceTimeoutPoller(
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val eventPublisherPort: EventPublisherPort,
) {
  private val log = LoggerFactory.getLogger(WaitingInstanceTimeoutPoller::class.java)

  @Scheduled(fixedDelayString = "\${triggerly.scheduler.timeout-poll-interval-ms:10000}")
  fun pollExpiredInstances() {
    val expired = workflowInstanceRepositoryPort.findWaitingExpired(LocalDateTime.now())
    expired.forEach { instance ->
      log.info("타임아웃 감지: instanceId=${instance.id} node=${instance.currentNodeId}")
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
    }
  }
}
```

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventInstanceRepositoryPort
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

// 스펙 5.1절: event_instance는 MySQL에 30일만 유지 (ES event_log는 계속 보관)
@Component
class EventInstanceCleanupJob(
  private val eventInstanceRepositoryPort: EventInstanceRepositoryPort,
  @Value("\${triggerly.scheduler.event-instance-retention-days:30}") private val retentionDays: Long,
) {
  private val log = LoggerFactory.getLogger(EventInstanceCleanupJob::class.java)

  @Scheduled(cron = "0 0 3 * * *")
  fun cleanup() {
    val deleted = eventInstanceRepositoryPort.deleteOlderThan(LocalDateTime.now().minusDays(retentionDays))
    log.info("EventInstance 정리: ${deleted}건 삭제 (retention=${retentionDays}일)")
  }
}
```

- [ ] **Step 7: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-bootstrap:test --tests "*WaitingInstanceTimeoutPollerTest*" --tests "*EventInstanceCleanupJobTest*"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 8: 앱이 기동되는지 수동 확인 (인프라 필요 — docker-compose는 Task 22에서 작성되므로, 여기서는 컴파일/부트 실패 여부만 확인)**

Run: `./gradlew :triggerly-bootstrap:bootJar`
Expected: `BUILD SUCCESSFUL` (JAR이 정상적으로 만들어짐 — 실제 기동은 Task 22 이후 docker-compose와 함께 확인)

- [ ] **Step 9: Commit**

```bash
git add triggerly-bootstrap
git commit -m "feat(bootstrap): add main application, timeout/cleanup schedulers, and config"
```

---

### Task 22: DemoRunner + docker-compose

**Files:**
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/DemoRunner.kt`
- Create: `triggerly-bootstrap/src/main/kotlin/com/seaotter/triggerly/bootstrap/DemoWorkflows.kt`
- Create: `docker-compose.yml`
- Create: `docker-compose.observability.yml`
- Create: `prometheus.yml`

**Interfaces:**
- Consumes: `ManageEventDefinitionUseCase`, `ManageAttributeDefinitionUseCase`, `ManageMemberUseCase`, `ManageWorkflowUseCase` (Task 6), `TriggerlyClient`(Task 23 — 이 태스크보다 먼저 작성해도 되고, Task 23을 먼저 끝내고 이 태스크를 해도 된다. 둘 다 서로의 산출물만 참조하므로 순서를 바꿔도 무방)
- Produces: `demo` 프로파일로 기동 시 4개 시나리오가 실제로 돌아가는 것을 콘솔 로그로 보여준다 — 이 프로젝트의 최종 데모 산출물.

- [ ] **Step 1: `DemoWorkflows.kt` — 4개 데모 시나리오의 `WorkflowDefinition` 빌더**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.domain.*
import kotlin.time.DurationUnit

object DemoWorkflows {

  // Trigger(CART_ADD) --Always--> WaitForEvent(PURCHASE, 1분) --Matched--> End
  //                                                            --Timeout--> Action(쿠폰) --Always--> End
  fun cartReminder() = WorkflowDefinition(
    trigger = "CART_ADD",
    nodes = listOf(
      Node.Trigger("n1", "CART_ADD"),
      Node.WaitForEvent("n2", WaitEventDefinition("PURCHASE", DurationDto(1, DurationUnit.MINUTES))),
      Node.End("n3"),
      Node.Action("n4", ActionDefinition.IssueCoupon("CART_REMIND10")),
      Node.End("n5"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Matched),
      Edge("n2", "n4", EdgeRoute.Timeout),
      Edge("n4", "n5", EdgeRoute.Always),
    ),
  )

  // Trigger(LOGIN) --Always--> Condition(생일=오늘) --True--> Action(쿠폰) --Always--> End
  //                                                  --False--> End
  fun birthdayCoupon() = WorkflowDefinition(
    trigger = "LOGIN",
    nodes = listOf(
      Node.Trigger("n1", "LOGIN"),
      Node.Condition("n2", ConditionExpression.Predicate("member.isBirthdayToday", ConditionOperator.EQ, true)),
      Node.Action("n3", ActionDefinition.IssueCoupon("BIRTHDAY10")),
      Node.End("n4"),
      Node.End("n5"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.True),
      Edge("n3", "n4", EdgeRoute.Always),
      Edge("n2", "n5", EdgeRoute.False),
    ),
  )

  // Trigger(SIGN_UP) --Always--> Delay(30초) --Always--> Action(알림톡) --Always--> End
  fun welcomeDelay() = WorkflowDefinition(
    trigger = "SIGN_UP",
    nodes = listOf(
      Node.Trigger("n1", "SIGN_UP"),
      Node.Delay("n2", DurationDto(30, DurationUnit.SECONDS)),
      Node.Action("n3", ActionDefinition.SendAlimTalk("welcome")),
      Node.End("n4"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Always),
      Edge("n3", "n4", EdgeRoute.Always),
    ),
  )

  // Trigger(PURCHASE) --Always--> WaitForEvent(REVIEW_ADD, 5분) --Matched--> Action(쿠폰10%) --Always--> End
  //                                                             --Timeout--> Condition(30일 리뷰수>=3)
  //                                                                    --True--> Action(알림톡) --Always--> End
  //                                                                    --False--> End
  fun reviewNudge() = WorkflowDefinition(
    trigger = "PURCHASE",
    nodes = listOf(
      Node.Trigger("n1", "PURCHASE"),
      Node.WaitForEvent("n2", WaitEventDefinition("REVIEW_ADD", DurationDto(5, DurationUnit.MINUTES))),
      Node.Action("n3", ActionDefinition.IssueCoupon("REVIEW10")),
      Node.End("n6"),
      Node.Condition("n4", ConditionExpression.Predicate("stats.eventCount:REVIEW_ADD:30d", ConditionOperator.GOE, 3)),
      Node.Action("n5", ActionDefinition.SendAlimTalk("review-nudge")),
      Node.End("n8"),
      Node.End("n7"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Matched),
      Edge("n3", "n6", EdgeRoute.Always),
      Edge("n2", "n4", EdgeRoute.Timeout),
      Edge("n4", "n5", EdgeRoute.True),
      Edge("n4", "n7", EdgeRoute.False),
      Edge("n5", "n8", EdgeRoute.Always),
    ),
  )
}
```

- [ ] **Step 2: `DemoRunner.kt` — `demo` 프로파일에서만 동작, SDK로 실제 HTTP 이벤트를 쏜다**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.usecase.ManageAttributeDefinitionUseCase
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.application.usecase.ManageMemberUseCase
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.domain.AttributeType
import com.seaotter.triggerly.domain.Member
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowStatus
import com.seaotter.triggerly.sdk.TriggerlyClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

@Component
@Profile("demo")
class DemoRunner(
  private val eventDefinitionUseCase: ManageEventDefinitionUseCase,
  private val attributeDefinitionUseCase: ManageAttributeDefinitionUseCase,
  private val workflowUseCase: ManageWorkflowUseCase,
  private val memberUseCase: ManageMemberUseCase,
  @Value("\${server.port:8080}") private val port: Int,
) : CommandLineRunner {

  private val log = LoggerFactory.getLogger(DemoRunner::class.java)
  private val tenantId = "demo-tenant"

  override fun run(vararg args: String?) {
    log.info("=== triggerly-claude 데모 시작 ===")
    seedDefinitions()
    seedMembers()
    seedWorkflows()

    val client = TriggerlyClient(baseUrl = "http://localhost:$port", apiKey = "demo-key")

    log.info("--- 시나리오 1: 장바구니 리마인드 (1분 안에 구매 안 하면 쿠폰) ---")
    client.sendEvent(tenantId, "CART_ADD", "member-1", mapOf("productId" to "P1"))

    log.info("--- 시나리오 2: 생일 쿠폰 (오늘 로그인) ---")
    client.sendEvent(tenantId, "LOGIN", "member-2")

    log.info("--- 시나리오 3: 가입 환영 알림 (30초 뒤 알림톡) ---")
    client.sendEvent(tenantId, "SIGN_UP", "member-3")

    log.info("--- 시나리오 4: 리뷰 유도 (5분 안에 리뷰 없으면 최근 30일 리뷰수 확인) ---")
    client.sendEvent(tenantId, "PURCHASE", "member-4", mapOf("amount" to 30000))

    log.info("=== 시드 완료. 콘솔에서 [DEMO ACTION] 로그를 지켜보세요 (최대 5분 소요) ===")
  }

  private fun seedDefinitions() {
    listOf(
      "CART_ADD" to "장바구니 담기", "PURCHASE" to "구매", "REVIEW_ADD" to "리뷰 작성",
      "LOGIN" to "로그인", "SIGN_UP" to "회원가입",
    ).forEach { (code, name) -> eventDefinitionUseCase.register(tenantId, code, name) }
    attributeDefinitionUseCase.register(tenantId, null, "amount", "결제 금액", AttributeType.LONG, true)
  }

  private fun seedMembers() {
    memberUseCase.register(Member(id = "member-1", tenantId = tenantId, externalMemberId = "member-1"))
    memberUseCase.register(
      Member(id = "member-2", tenantId = tenantId, externalMemberId = "member-2", birthday = LocalDate.now()),
    )
    memberUseCase.register(Member(id = "member-3", tenantId = tenantId, externalMemberId = "member-3"))
    memberUseCase.register(Member(id = "member-4", tenantId = tenantId, externalMemberId = "member-4"))
  }

  private fun seedWorkflows() {
    listOf(
      Triple("장바구니 리마인드", "CART_ADD", DemoWorkflows.cartReminder()),
      Triple("생일 쿠폰", "LOGIN", DemoWorkflows.birthdayCoupon()),
      Triple("가입 환영 알림", "SIGN_UP", DemoWorkflows.welcomeDelay()),
      Triple("리뷰 유도", "PURCHASE", DemoWorkflows.reviewNudge()),
    ).forEach { (name, triggerEventCode, definition) ->
      val workflow = Workflow(
        id = UUID.randomUUID().toString(), tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
        definitionJson = definition, status = WorkflowStatus.DRAFT,
        createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
      )
      workflowUseCase.create(workflow)
      workflowUseCase.enable(tenantId, workflow.id)
    }
  }
}
```

- [ ] **Step 3: `docker-compose.yml` (루트 디렉터리)**

```yaml
services:
  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: triggerly
      MYSQL_USER: triggerly
      MYSQL_PASSWORD: triggerly
    ports:
      - "3306:3306"

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9200:9200"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  kafka:
    image: apache/kafka:3.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
```

- [ ] **Step 4: `docker-compose.observability.yml` + `prometheus.yml` (선택 오버레이, 13.5절)**

```yaml
services:
  prometheus:
    image: prom/prometheus:v2.53.0
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:11.1.0
    ports:
      - "3000:3000"
```

```yaml
scrape_configs:
  - job_name: triggerly-claude
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["host.docker.internal:8080"]
```

- [ ] **Step 5: 인프라 기동 후 앱을 데모 프로파일로 실행해 수동 검증**

Run:
```bash
docker compose up -d
sleep 20   # ES/Kafka 초기화 대기
./gradlew :triggerly-bootstrap:bootRun --args='--spring.profiles.active=local,demo'
```
Expected: 콘솔에 `=== triggerly-claude 데모 시작 ===` 로그 이후, 시나리오 2(생일 쿠폰)는 즉시 `[DEMO ACTION] 쿠폰 발급: couponId=BIRTHDAY10`이 찍히고, 시나리오 3(가입 환영)은 약 30초 후 `[DEMO ACTION] 알림톡 발송: templateId=welcome`이 찍힌다. 시나리오 1/4는 각각 1분/5분 후 타임아웃 경로 로그가 찍힌다(중간에 `PURCHASE`/`REVIEW_ADD` 이벤트를 수동으로 보내면 Matched 경로도 확인 가능 — Task 26의 `scripts/demo.sh` 참고).

- [ ] **Step 6: Commit**

```bash
git add triggerly-bootstrap docker-compose.yml docker-compose.observability.yml prometheus.yml
git commit -m "feat(bootstrap): add DemoRunner seeding 4 end-to-end workflow scenarios and docker-compose infra"
```

---

### Task 23: triggerly-sdk

**Files:**
- Create: `triggerly-sdk/src/main/kotlin/com/seaotter/triggerly/sdk/TriggerlyClient.kt`
- Test: `triggerly-sdk/src/test/kotlin/com/seaotter/triggerly/sdk/TriggerlyClientTest.kt`

**Interfaces:**
- Consumes: 없음 (domain/application 비의존 — 독립 라이브러리)
- Produces: `TriggerlyClient(baseUrl, apiKey).sendEvent(tenantId, eventCode, externalMemberId?, attributes?)` — Task 22(DemoRunner)와 Task 26(README 예시)이 사용.

- [ ] **Step 1: 실패하는 테스트 작성 (JDK 내장 `com.sun.net.httpserver.HttpServer`를 스텁 서버로 사용 — 추가 의존성 없음)**

```kotlin
package com.seaotter.triggerly.sdk

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TriggerlyClientTest {

  private lateinit var server: HttpServer
  private var lastRequestBody: String = ""
  private var lastPath: String = ""
  private var responseStatus = 202

  @BeforeTest
  fun startServer() {
    responseStatus = 202
    server = HttpServer.create(InetSocketAddress(0), 0)
    server.createContext("/api/v1/events") { exchange ->
      lastPath = exchange.requestURI.path
      lastRequestBody = exchange.requestBody.readBytes().decodeToString()
      exchange.sendResponseHeaders(responseStatus, -1)
      exchange.close()
    }
    server.start()
  }

  @AfterTest
  fun stopServer() {
    server.stop(0)
  }

  @Test
  fun `sendEvent는 POST api v1 events로 JSON 바디를 전송한다`() {
    val client = TriggerlyClient(baseUrl = "http://localhost:${server.address.port}", apiKey = "key")

    client.sendEvent("t1", "PURCHASE", "ext-1", mapOf("amount" to 1000))

    assertEquals("/api/v1/events", lastPath)
    assertTrue(lastRequestBody.contains("PURCHASE"))
    assertTrue(lastRequestBody.contains("ext-1"))
  }

  @Test
  fun `2xx가 아닌 응답이면 TriggerlyClientException을 던진다`() {
    responseStatus = 500
    val client = TriggerlyClient(baseUrl = "http://localhost:${server.address.port}", apiKey = "key")

    assertFailsWith<TriggerlyClientException> { client.sendEvent("t1", "PURCHASE") }
  }
}
```

- [ ] **Step 2: 테스트 실행하여 실패 확인**

Run: `./gradlew :triggerly-sdk:test --tests "*TriggerlyClientTest*"`
Expected: FAIL (컴파일 에러 — `TriggerlyClient` 없음)

- [ ] **Step 3: 구현**

```kotlin
package com.seaotter.triggerly.sdk

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

data class SdkMemberContext(val externalMemberId: String)

data class SdkEventRequest(
  val tenantId: String,
  val eventCode: String,
  val member: SdkMemberContext? = null,
  val attributes: Map<String, Any?>? = null,
)

class TriggerlyClientException(message: String) : RuntimeException(message)

class TriggerlyClient(
  private val baseUrl: String,
  private val apiKey: String,
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
) {
  private val mapper = jacksonObjectMapper()

  fun sendEvent(
    tenantId: String,
    eventCode: String,
    externalMemberId: String? = null,
    attributes: Map<String, Any?>? = null,
  ) {
    val body = SdkEventRequest(
      tenantId = tenantId,
      eventCode = eventCode,
      member = externalMemberId?.let { SdkMemberContext(it) },
      attributes = attributes,
    )
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/api/v1/events"))
      .header("Content-Type", "application/json")
      .header("Authorization", "Bearer $apiKey")
      .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() !in 200..299) {
      throw TriggerlyClientException("이벤트 전송 실패: status=${response.statusCode()} body=${response.body()}")
    }
  }
}
```

- [ ] **Step 4: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-sdk:test --tests "*TriggerlyClientTest*"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: Commit**

```bash
git add triggerly-sdk
git commit -m "feat(sdk): add lightweight TriggerlyClient for customer apps"
```

---

### Task 24: 통합 테스트 (Testcontainers 풀스택)

**Files:**
- Create: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/FullStackIntegrationTest.kt`
- Test: `triggerly-bootstrap/src/test/kotlin/com/seaotter/triggerly/bootstrap/CartReminderScenarioIntegrationTest.kt`

**Interfaces:**
- Consumes: 전체 애플리케이션 (`TriggerlyClaudeApplication`, Task 21) — MySQL/ES/Redis/Kafka 4개 컨테이너를 모두 띄운다.
- Produces: 스펙 11절이 요구하는 "이벤트 수집 API 호출 → Kafka 소비 → 엔진 실행 → DB 상태 반영" 전체 흐름의 Matched/Timeout 각 1개 시나리오 검증. 이게 통과하면 데모가 실제로 동작한다는 최종 증거가 된다.

- [ ] **Step 1: `FullStackIntegrationTest.kt` (베이스 — 4개 인프라 전부)**

```kotlin
package com.seaotter.triggerly.bootstrap

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@Testcontainers
@SpringBootTest(
  classes = [TriggerlyClaudeApplication::class],
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
abstract class FullStackIntegrationTest {

  companion object {
    @Container
    @ServiceConnection
    @JvmStatic
    val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4").apply { withDatabaseName("triggerly") }

    @Container
    @ServiceConnection
    @JvmStatic
    val elasticsearch: ElasticsearchContainer =
      ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.15.0")
        .apply { withEnv("xpack.security.enabled", "false") }

    @Container
    @ServiceConnection
    @JvmStatic
    val kafka: KafkaContainer = KafkaContainer("apache/kafka:3.7.0")

    @Container
    @JvmStatic
    val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379)

    @JvmStatic
    @DynamicPropertySource
    fun registerRedisProperties(registry: DynamicPropertyRegistry) {
      registry.add("spring.data.redis.host") { redis.host }
      registry.add("spring.data.redis.port") { redis.getMappedPort(6379) }
    }
  }
}
```

- [ ] **Step 2: 실패하는 테스트 작성 (Matched 시나리오 + Timeout 시나리오)**

```kotlin
package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.adapter.persistence.jpa.WorkflowInstanceJpaRepository
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.application.usecase.WorkflowInstanceQueryUseCase
import com.seaotter.triggerly.domain.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.HttpStatus
import org.springframework.test.context.TestPropertySource
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.time.DurationUnit

@TestPropertySource(properties = ["triggerly.scheduler.timeout-poll-interval-ms=2000"])
class CartReminderScenarioIntegrationTest : FullStackIntegrationTest() {

  @Autowired lateinit var eventDefinitionUseCase: ManageEventDefinitionUseCase
  @Autowired lateinit var workflowUseCase: ManageWorkflowUseCase
  @Autowired lateinit var workflowInstanceQueryUseCase: WorkflowInstanceQueryUseCase
  @Autowired lateinit var workflowInstanceJpaRepository: WorkflowInstanceJpaRepository
  @Autowired lateinit var restTemplate: TestRestTemplate

  @LocalServerPort
  var port: Int = 0

  private fun cartReminderWorkflow() = WorkflowDefinition(
    trigger = "CART_ADD",
    nodes = listOf(
      Node.Trigger("n1", "CART_ADD"),
      Node.WaitForEvent("n2", WaitEventDefinition("PURCHASE", DurationDto(2, DurationUnit.SECONDS))),
      Node.End("n3"),
      Node.Action("n4", ActionDefinition.IssueCoupon("IT10")),
      Node.End("n5"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Matched),
      Edge("n2", "n4", EdgeRoute.Timeout),
      Edge("n4", "n5", EdgeRoute.Always),
    ),
  )

  private fun seed(tenantId: String): String {
    eventDefinitionUseCase.register(tenantId, "CART_ADD", "장바구니")
    eventDefinitionUseCase.register(tenantId, "PURCHASE", "구매")
    val workflow = Workflow(
      id = UUID.randomUUID().toString(), tenantId = tenantId, triggerEventCode = "CART_ADD",
      definitionJson = cartReminderWorkflow(), status = WorkflowStatus.DRAFT,
      createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )
    workflowUseCase.create(workflow)
    workflowUseCase.enable(tenantId, workflow.id)
    return workflow.id
  }

  private fun postEvent(tenantId: String, eventCode: String, externalMemberId: String) {
    val body = mapOf(
      "tenantId" to tenantId, "eventCode" to eventCode,
      "member" to mapOf("externalMemberId" to externalMemberId),
    )
    val response = restTemplate.postForEntity("http://localhost:$port/api/v1/events", body, String::class.java)
    assertEquals(HttpStatus.ACCEPTED, response.statusCode)
  }

  private fun awaitCompletedInstance(workflowId: String, timeoutSeconds: Long): String {
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000
    while (System.currentTimeMillis() < deadline) {
      val completed = workflowInstanceJpaRepository.findAll()
        .firstOrNull { it.workflowId == workflowId && it.status == "COMPLETED" }
      if (completed != null) return completed.id
      Thread.sleep(300)
    }
    error("워크플로 인스턴스가 시간 내에 COMPLETED되지 않음: workflowId=$workflowId")
  }

  @Test
  fun `PURCHASE 이벤트가 타임아웃 전에 오면 Matched 경로로 완료된다`() {
    val tenantId = "it-matched-${UUID.randomUUID()}"
    val workflowId = seed(tenantId)

    postEvent(tenantId, "CART_ADD", "member-matched")
    Thread.sleep(500)
    postEvent(tenantId, "PURCHASE", "member-matched")

    val instanceId = awaitCompletedInstance(workflowId, timeoutSeconds = 10)
    val view = workflowInstanceQueryUseCase.get(instanceId)!!
    assertEquals(WorkflowInstanceStatus.COMPLETED, view.instance.status)
    assertEquals("n3", view.instance.currentNodeId)
  }

  @Test
  fun `PURCHASE 이벤트가 안 오면 스케줄러가 타임아웃시켜 쿠폰 액션으로 완료시킨다`() {
    val tenantId = "it-timeout-${UUID.randomUUID()}"
    val workflowId = seed(tenantId)

    postEvent(tenantId, "CART_ADD", "member-timeout")

    val instanceId = awaitCompletedInstance(workflowId, timeoutSeconds = 20)
    val view = workflowInstanceQueryUseCase.get(instanceId)!!
    assertEquals(WorkflowInstanceStatus.COMPLETED, view.instance.status)
    assertEquals("n5", view.instance.currentNodeId)
    assertEquals(true, view.executions.any { it.nodeId == "n4" && it.result?.contains("IT10") == true })
  }
}
```

- [ ] **Step 3: 테스트 실행하여 실패 확인 (Docker 데몬 필요)**

Run: `./gradlew :triggerly-bootstrap:test --tests "*CartReminderScenarioIntegrationTest*"`
Expected: FAIL — 이 시점까지 모든 모듈이 이미 구현돼 있으므로 컴파일은 되지만, 배선 문제(빈 이름, 프로퍼티 키 오타 등)가 있다면 여기서 처음 드러난다. 실패하면 로그를 보고 어느 어댑터/설정이 빠졌는지 확인 후 수정한다.

- [ ] **Step 4: 실패 원인 수정 (전형적인 문제와 대처)**

- Flyway 마이그레이션 실패 → `triggerly-bootstrap/src/main/resources/db/migration/V1__init.sql` 경로/문법 확인
- `ddl-auto: validate` 에러 → JPA 엔티티 필드와 `V1__init.sql` 컬럼명이 정확히 일치하는지 확인 (특히 `attrKey`→`attr_key`, `attrType`→`attr_type` 같은 카멜→스네이크 매핑)
- Kafka 컨슈머가 메시지를 못 받음 → `JsonDeserializer.TRUSTED_PACKAGES` 값과 실제 `RawEventMessage` 패키지 일치 확인
- 이 스텝은 반복적 디버깅이므로 고정된 코드 스니펫이 아니라, 테스트가 통과할 때까지 원인을 찾아 수정하는 작업이다.

- [ ] **Step 5: 테스트 실행하여 통과 확인**

Run: `./gradlew :triggerly-bootstrap:test --tests "*CartReminderScenarioIntegrationTest*"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed (Timeout 테스트는 최대 20초 정도 소요될 수 있음)

- [ ] **Step 6: 전체 프로젝트 빌드 확인**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL` — 7개 모듈 전체의 단위/통합 테스트가 모두 통과

- [ ] **Step 7: Commit**

```bash
git add triggerly-bootstrap
git commit -m "test(bootstrap): add full-stack integration tests for cart-reminder matched/timeout paths"
```

---

### Task 25: 부하테스트 도구 (k6)

**Files:**
- Create: `loadtest/ingest.js`
- Create: `loadtest/README.md`

**Interfaces:**
- Consumes: `POST /api/v1/events` (Task 17)
- Produces: 사용자가 직접 rps를 바꿔가며 실행할 수 있는 k6 스크립트 — 스펙 13.5절. 자동화 테스트 스위트에는 포함하지 않는다.

- [ ] **Step 1: `loadtest/ingest.js` 작성**

```javascript
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '5', 10);
const MEMBER_COUNT = parseInt(__ENV.MEMBER_COUNT || '1000', 10);
const EVENT_CODE = __ENV.EVENT_CODE || 'CART_ADD';

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.TARGET_RPS || '100', 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: parseInt(__ENV.VUS || '50', 10),
      maxVUs: parseInt(__ENV.MAX_VUS || '200', 10),
    },
  },
};

export default function () {
  const tenantId = `loadtest-tenant-${Math.floor(Math.random() * TENANT_COUNT)}`;
  const externalMemberId = `member-${Math.floor(Math.random() * MEMBER_COUNT)}`;

  const payload = JSON.stringify({
    tenantId,
    eventCode: EVENT_CODE,
    member: { externalMemberId },
    attributes: { amount: Math.floor(Math.random() * 100000) },
  });

  const res = http.post(`${BASE_URL}/api/v1/events`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202 or 429': (r) => r.status === 202 || r.status === 429,
  });
}
```

- [ ] **Step 2: `loadtest/README.md` 작성**

```markdown
# triggerly-claude 부하테스트

[k6](https://k6.io/) 설치 필요: `brew install k6`

## 사전 준비

1. `docker compose up -d`로 인프라 기동, 앱은 `local` 프로파일로 기동
2. 사용할 `tenantId`마다 `EventDefinition`을 먼저 등록해야 202를 받는다 (미등록이면 400):
   ```bash
   for i in 0 1 2 3 4; do
     curl -X POST http://localhost:8080/api/v1/admin/event-definitions \
       -H "Content-Type: application/json" \
       -d "{\"tenantId\":\"loadtest-tenant-$i\",\"code\":\"CART_ADD\",\"displayName\":\"장바구니\"}"
   done
   ```

## 단계적으로 rps 올려보기

```bash
TARGET_RPS=100 DURATION=30s k6 run loadtest/ingest.js
TARGET_RPS=1000 DURATION=1m VUS=200 MAX_VUS=500 k6 run loadtest/ingest.js
# 로컬 리소스 한계에 부딪히면 EC2 등 별도 환경에서 계속 올려보세요 (스펙 14절 — 실제 수만 rps 검증은 범위 밖)
TARGET_RPS=10000 DURATION=1m VUS=1000 MAX_VUS=2000 k6 run loadtest/ingest.js
```

## 튜닝 포인트 (`application.yml`)

- `triggerly.kafka.raw-events-topic.partitions` — 늘리면 컨슈머 병렬성 상한이 늘어남
- `triggerly.kafka.consumer.concurrency` — 컨슈머 스레드 수 (파티션 수 이하로)
- `triggerly.ratelimit.default-rps` — 429가 너무 많으면 올려서 재시도
- `triggerly.async.member-sync.*` — ES 동기화 풀 크기
- `docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d` 후 Grafana(`localhost:3000`)에서 `/actuator/prometheus` 지표를 보며 조정
```

- [ ] **Step 3: 스크립트 문법 확인 (k6가 설치돼 있으면)**

Run: `k6 run --vus 1 --duration 1s loadtest/ingest.js` (앱이 안 떠 있으면 실패해도 무방 — 스크립트 문법 자체만 확인)
Expected: k6가 스크립트를 파싱하고 실행을 시도함 (문법 에러 없음)

- [ ] **Step 4: Commit**

```bash
git add loadtest
git commit -m "docs: add k6 load test script and tuning guide"
```

---

### Task 26: 데모 스크립트 + README

**Files:**
- Create: `scripts/demo.sh`
- Create: `README.md`

**Interfaces:**
- Consumes: `POST /api/v1/admin/event-definitions`, `POST /api/v1/admin/workflows`, `POST /api/v1/admin/workflows/{id}/enable`, `POST /api/v1/events` (Task 17~19)
- Produces: SDK/Kotlin 없이 curl만으로 데모 시나리오 1(장바구니 리마인드)을 재현하는 스크립트. 프로젝트 최상위 README.

- [ ] **Step 1: `scripts/demo.sh` 작성**

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
TENANT_ID="demo-tenant-cli"
WORKFLOW_ID="demo-cli-workflow"

echo "1) 이벤트 정의 등록"
curl -s -X POST "$BASE_URL/api/v1/admin/event-definitions" -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"$TENANT_ID\",\"code\":\"CART_ADD\",\"displayName\":\"장바구니 담기\"}" > /dev/null
curl -s -X POST "$BASE_URL/api/v1/admin/event-definitions" -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"$TENANT_ID\",\"code\":\"PURCHASE\",\"displayName\":\"구매\"}" > /dev/null

echo "2) 워크플로 등록: CART_ADD -> WaitForEvent(PURCHASE, 1분) -> Matched:End / Timeout:쿠폰"
curl -s -X POST "$BASE_URL/api/v1/admin/workflows" -H "Content-Type: application/json" -d '{
  "id": "'"$WORKFLOW_ID"'",
  "tenantId": "'"$TENANT_ID"'",
  "name": "장바구니 리마인드 (CLI)",
  "triggerEventCode": "CART_ADD",
  "definitionJson": {
    "trigger": "CART_ADD",
    "nodes": [
      {"type":"TRIGGER","id":"n1","eventCode":"CART_ADD"},
      {"type":"WAIT_FOR_EVENT","id":"n2","event":{"eventCode":"PURCHASE","timeout":{"value":1,"unit":"MINUTES"}}},
      {"type":"END","id":"n3"},
      {"type":"ACTION","id":"n4","action":{"type":"ISSUE_COUPON","couponId":"CLI10"}},
      {"type":"END","id":"n5"}
    ],
    "edges": [
      {"from":"n1","to":"n2","route":{"type":"ALWAYS"}},
      {"from":"n2","to":"n3","route":{"type":"MATCHED"}},
      {"from":"n2","to":"n4","route":{"type":"TIMEOUT"}},
      {"from":"n4","to":"n5","route":{"type":"ALWAYS"}}
    ]
  }
}' > /dev/null

curl -s -X POST "$BASE_URL/api/v1/admin/workflows/$WORKFLOW_ID/enable?tenantId=$TENANT_ID" > /dev/null

echo "3) CART_ADD 이벤트 전송"
curl -s -X POST "$BASE_URL/api/v1/events" -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"$TENANT_ID\",\"eventCode\":\"CART_ADD\",\"member\":{\"externalMemberId\":\"cli-member-1\"}}"
echo ""
echo "1분 안에 아래 명령으로 PURCHASE를 보내면 Matched 경로, 안 보내면 1분 뒤 쿠폰 액션(Timeout 경로)이 앱 로그에 찍힙니다:"
echo "  curl -X POST $BASE_URL/api/v1/events -H 'Content-Type: application/json' -d '{\"tenantId\":\"$TENANT_ID\",\"eventCode\":\"PURCHASE\",\"member\":{\"externalMemberId\":\"cli-member-1\"}}'"
```

- [ ] **Step 2: 실행 권한 부여 및 문법 확인**

Run: `chmod +x scripts/demo.sh && bash -n scripts/demo.sh`
Expected: 문법 에러 없이 종료 (`bash -n`은 실제 실행 없이 문법만 검사)

- [ ] **Step 3: 최상위 `README.md` 작성**

```markdown
# triggerly-claude

이벤트 트리거 워크플로 플랫폼(triggerly)의 데모 구현체입니다.
- 설계 배경: `docs/superpowers/specs/2026-09-01-triggerly-claude-demo-design.md`
- 구현 계획: `docs/superpowers/plans/2026-09-02-triggerly-claude-demo-implementation.md`

## 요구 사항

- Java 21 (`.java-version` 참고)
- Docker / Docker Compose
- (선택) [k6](https://k6.io/) — 부하테스트용

## 빠르게 실행하기 (자동 데모)

```bash
docker compose up -d          # MySQL, Elasticsearch, Redis, Kafka
sleep 20                      # ES/Kafka 초기화 대기

./gradlew :triggerly-bootstrap:bootRun --args='--spring.profiles.active=local,demo'
```

콘솔에서 `[DEMO ACTION]` 로그가 찍히는 걸 지켜보세요 (즉시~5분 소요, 시나리오별 상세는 스펙 8절).

## 수동으로 시나리오 재현하기

```bash
./gradlew :triggerly-bootstrap:bootRun --args='--spring.profiles.active=local'
./scripts/demo.sh
```

## API 문서

앱 기동 후 http://localhost:8080/swagger-ui.html

## 부하테스트

`loadtest/README.md` 참고.

## 관측 (선택)

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```
Grafana: http://localhost:3000, Prometheus: http://localhost:9090

## 모듈 구조

구현 계획 문서의 "File Structure" 절 참고.
```

- [ ] **Step 4: Commit**

```bash
git add scripts/demo.sh README.md
git commit -m "docs: add demo.sh reproduction script and top-level README"
```

---

## 최종 검증

- [ ] `./gradlew build` — 전체 7개 모듈 단위/통합 테스트 통과
- [ ] `docker compose up -d` 후 `./gradlew :triggerly-bootstrap:bootRun --args='--spring.profiles.active=local,demo'`로 4개 시나리오가 콘솔 로그에 전부 찍히는 것을 육안으로 확인
- [ ] `./scripts/demo.sh` 실행 후 수동으로 PURCHASE 이벤트를 보내 Matched 경로도 확인
- [ ] `http://localhost:8080/swagger-ui.html`에서 전체 API 확인


















