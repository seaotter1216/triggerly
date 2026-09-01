# triggerly-claude 데모 설계 문서

- 작성일: 2026-09-01
- 대상: `/Users/ben/BenDev/playground/triggerly-claude`
- 참고 원본: `/Users/ben/BenDev/playground/triggerly` (헥사고날 아키텍처 스켈레톤, `triggerly-domain/src/test/kotlin`의 `Event.kt`/`Member.kt`/`Workflow.kt`가 도메인 모델 초안)

## 1. 목표

CDP/CRM성 이벤트 트리거 워크플로 플랫폼(triggerly)의 핵심 흐름이 실제로 동작하는 데모를 만든다.

1. 고객사 회원 정보를 MySQL(정합성)과 Elasticsearch(조회 성능)에 이중으로 저장
2. 어드민이 이벤트 정의(`EventDefinition`), 속성 정의(`AttributeDefinition`), 워크플로 정의(`WorkflowDefinition`, JSON)를 등록
3. 고객사 앱이 `triggerly-sdk`(또는 REST API)로 이벤트를 전송하면 워크플로가 실제로 트리거되어 진행
4. 이벤트 발생 시 DB에 저장된 workflow JSON을 조회하여 노드(TRIGGER/CONDITION/ACTION/WAIT_FOR_EVENT/DELAY/END)를 순회하며 실행하는 엔진
5. `triggerly-domain`의 기존 테스트 스케치 클래스를 실제 `src/main` 도메인 모델로 승격·확장

범위는 "데모가 실제로 돌아가는 것을 보여주는 것"이며, 상용 수준의 멀티테넌시 격리, 인증/인가, 장애 복구는 다루지 않는다. 다만 테넌트당 초당 수만 건 트래픽을 견디는 아키텍처(스레드/메모리 방어, Kafka 파티셔닝, 수평 확장 가능한 컨슈머)는 13절에 따라 설계에 포함하고, 실제 수치로 로컬에서 부하테스트까지 돌리는 것은 사용자가 직접 진행한다 (14절 참고).

## 2. 모듈 구성

기존 `triggerly`와 동일한 헥사고날 레이어링을 따르되, 메시징 어댑터와 SDK를 추가한다.

```
triggerly-claude/
├── docker-compose.yml                    # MySQL, Elasticsearch, Redis, Kafka(KRaft)
├── settings.gradle.kts
├── build.gradle.kts
├── gradle/libs.versions.toml
├── triggerly-domain/                     # 순수 도메인 모델 + 조건식 평가기 (프레임워크 비의존)
├── triggerly-application/                # 유스케이스 + WorkflowEngine + 포트(인터페이스)
├── triggerly-adapter-in-web/              # REST 컨트롤러 (이벤트 수집 API, 어드민 API)
├── triggerly-adapter-out-persistence/     # JPA(MySQL) + Spring Data Elasticsearch
├── triggerly-adapter-messaging/           # Kafka producer/consumer + Redis 대기 인덱스 (신규 모듈)
├── triggerly-bootstrap/                   # Spring Boot 메인, 설정, 스케줄러, Flyway 마이그레이션
├── triggerly-sdk/                         # 고객사 앱용 독립 경량 클라이언트 (domain 비의존)
├── loadtest/                              # k6 부하테스트 스크립트 (Gradle 모듈 아님)
└── docker-compose.observability.yml       # Prometheus + Grafana 오버레이 (선택)
```

의존 방향: `adapter-* , bootstrap → application → domain`. `triggerly-sdk`는 "고객사 코드" 역할이므로 다른 모든 모듈과 독립이며 자체 DTO만 가진다.

기술 스택: Kotlin 2.3.21 / Spring Boot 4.1.0 / Java 21 (기존 `libs.versions.toml` 값 그대로 사용). 추가 필요 의존성: Spring Data JPA + MySQL Connector/J, Spring Data Elasticsearch(또는 co.elastic.clients 저수준 클라이언트), Spring for Apache Kafka, Spring Data Redis(Lettuce), Flyway, Bucket4j-Redis(테넌트 레이트리밋), Micrometer Prometheus Registry + Spring Boot Actuator.

## 3. 도메인 모델 (승격 대상)

`triggerly-domain/src/test/kotlin`의 세 파일을 `src/main/kotlin`으로 옮기고 다음을 보강한다.

- `Event.kt`: `EventDefinition`, `EventRequest`, `EventInstance`, `AttributeDefinition`, `AttributeType` — 원안 유지
- `Member.kt`: `Member`, `MemberContext`, `MemberStatus`, `Gender`, `DevicePlatform` — 원안 유지 (테스트 파일에 섞여있던 `BirthdayTest`는 별도 테스트 클래스로 분리)
- `Workflow.kt`: `Workflow`, `WorkflowInstance`, `WorkflowExecution`, `WorkflowDefinition`, `Node`(sealed interface: Trigger/Condition/Action/WaitForEvent/Delay/End), `Edge`, `EdgeRoute`, `ConditionExpression`(Predicate/And/Or/Not), `ActionDefinition`(IssueCoupon/SendPush/SendAlimTalk), `WaitEventDefinition`, `DurationDto` — 원안 유지
- **신규 추가**: `ConditionEvaluator` — `ConditionExpression`을 평가 컨텍스트(`Map<String, Any?>`)에 대해 계산하는 순수 함수 객체. 4절에서 상세 정의.
- **신규 추가**: `WorkflowExecutionStatus`가 `Workflow.kt`에 이미 있으나 `WorkflowExecution.status` 필드 타입이 원안에서 `WorkflowStatus`로 되어 있는 버그를 `WorkflowExecutionStatus`로 수정한다 (원안 오타로 판단).

## 4. 조건식 평가기 (ConditionEvaluator)

`ConditionExpression.Predicate(field, operator, value)`의 `field`는 두 가지 형태를 지원한다.

- **컨텍스트 필드**: `"event.amount"`, `"member.city"`, `"member.attributes.grade"` 등 `.`으로 구분된 경로. 평가 시점에 전달되는 `Map<String, Any?>` 컨텍스트(트리거 이벤트의 attributes + 회원 attributes/필드를 병합한 맵)에서 값을 꺼낸다.
- **집계 필드**: `"stats.eventCount:<EVENT_CODE>:<N>d"` 형태 (예: `"stats.eventCount:REVIEW_ADD:30d"`). evaluator가 이 패턴(정규식 `^stats\.eventCount:([A-Z0-9_]+):(\d+)d$`)을 인식하면 로컬 컨텍스트가 아니라 `MemberEventStatsPort.countEvents(memberId, eventCode, withinDays)`를 호출해 Elasticsearch `event_log` 인덱스에서 count 쿼리를 수행한다.

연산자(`ConditionOperator`: EQ, NE, GT, GOE, LT, LOE, IN, NOT_IN, EXISTS)는 값 타입에 따라 표준 비교를 수행한다. 숫자 비교는 `Number`로 통일 변환 후 비교, `IN`/`NOT_IN`은 `value`가 `List<*>`여야 한다, `EXISTS`는 컨텍스트에 키 존재 여부만 확인한다.

`ConditionEvaluator`의 시그니처(도메인 계층, 순수):
```kotlin
fun interface EventStatsResolver {
  fun countEvents(memberId: String, eventCode: String, withinDays: Int): Long
}

object ConditionEvaluator {
  fun evaluate(
    expr: ConditionExpression,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver
  ): Boolean
}
```
`domain`은 ES를 직접 알지 못하므로 `EventStatsResolver`라는 도메인 인터페이스를 정의하고, `application` 계층의 `MemberEventStatsPort`(ES 어댑터가 구현)가 이를 실제로 구현한 어댑터를 주입한다. (엄밀히는 domain의 함수형 인터페이스를 application이 구현체로 감싸 전달)

## 5. 데이터 모델

### 5.1 MySQL (Flyway 마이그레이션, `triggerly-bootstrap/src/main/resources/db/migration`)

- `event_definition(id, tenant_id, code, display_name, created_at)` — unique(tenant_id, code)
- `attribute_definition(id, tenant_id, event_definition_id NULL, attr_key, display_name, attr_type, filterable, created_at)`
- `member(id, tenant_id, external_member_id, name, email, telephone, device_platform, gender, birthday, status, joined_at, last_login_at, withdrawn_at, created_at, marketing_sms_agreed, marketing_push_agreed, marketing_email_agreed, marketing_kakao_agreed, marketing_agreed_at, attributes JSON)` — unique(tenant_id, external_member_id)
- `event_instance(id, event_code, occurred_at, member_id NULL, attributes JSON, created_at)` — 매일 새벽 `@Scheduled` 배치가 `occurred_at < now() - 30d`인 행을 삭제 (ES는 유지)
- `workflow(id, tenant_id, name, trigger_event_code, definition_json JSON, version, status, created_at, last_updated_at)`
- `workflow_instance(id, workflow_id, tenant_id, trigger_event_code, version, member_id NULL, status, current_node_id NULL, waiting_event_name NULL, waiting_until NULL, started_at NULL, completed_at NULL)` — 인덱스: (status, waiting_until) — 스케줄러 폴링용
- `workflow_execution(id, workflow_instance_id, node_id, node_type, status, started_at NULL, completed_at NULL, result NULL, error_code NULL)`

### 5.2 Elasticsearch

- `member` 인덱스: MySQL member 테이블과 동일 필드 + `attributes`(dynamic object). MySQL 저장 성공 후 비동기(Spring `@Async` + `ApplicationEventPublisher`)로 반영. 세그먼트/속성 기반 검색 API(`GET /api/v1/admin/members/search`)가 이 인덱스를 조회.
- `event_log` 인덱스: `id, tenantId, eventCode, occurredAt, memberId, attributes`. 이벤트 수집 시 MySQL과 함께 색인 (Kafka 컨슈머 처리 단계에서 저장). `stats.eventCount` 조건 평가의 조회 대상.

### 5.3 Redis

- Key: `waiting:event:{eventCode}:member:{memberId}` → Value: List, 각 원소는 `workflowInstanceId`. WAIT_FOR_EVENT 노드 진입 시 등록, Matched 또는 Timeout으로 해당 인스턴스가 빠져나갈 때 리스트에서 제거.

### 5.4 Kafka

- 토픽 `triggerly.events.raw` (단일 토픽, 파티션 수는 `triggerly.kafka.raw-events-topic.partitions`로 설정, 기본 32). 파티션 키 = `tenantId:memberId` 로 회원 단위 순서를 보장하면서, 회원 수가 많은 테넌트는 자연히 여러 파티션에 분산되어 특정 테넌트가 파티션 하나를 독점하지 않게 함 (상세 근거는 13절)
- 메시지 payload: `{ tenantId, eventCode, memberId?, attributes?, occurredAt, syntheticTimeoutForInstanceId? }`
  - 일반 이벤트: `syntheticTimeoutForInstanceId`는 null
  - 스케줄러가 만드는 타임아웃 이벤트: `eventCode = "__TIMEOUT__"`, `syntheticTimeoutForInstanceId`에 대상 `workflowInstanceId` 설정 — 컨슈머는 이 필드가 있으면 트리거 매칭/역색인 매칭을 건너뛰고 해당 인스턴스를 직접 Timeout 경로로 재개
- 컨슈머는 `@KafkaListener(concurrency = "${triggerly.kafka.consumer.concurrency}")`(기본 8)로 고정 스레드 수를 유지하며, `max.poll.records`(기본 500)로 배치 크기를 제한해 MySQL/ES에 벌크로 저장

## 6. 이벤트 흐름

1. 고객사 앱 → `triggerly-sdk`의 `TriggerlyClient.sendEvent(EventRequest)` (내부적으로 `java.net.http.HttpClient`로 REST 호출) → `POST /api/v1/events`
2. 컨트롤러(가상 스레드로 처리, 13.1절)는 Redis 기반 테넌트별 레이트리밋을 먼저 확인(초과 시 `429` 즉시 반환, 13.2절), 통과하면 요청을 검증(등록된 `EventDefinition`인지 확인)하고 즉시 `202 Accepted` 응답 후, `EventPublisherPort` 구현체(Kafka producer)로 `triggerly.events.raw`에 논블로킹 produce
3. `triggerly-adapter-messaging`의 Kafka 컨슈머(`WorkflowTriggerConsumer`)가 메시지 소비:
   a. `syntheticTimeoutForInstanceId`가 없으면: `EventInstance`를 MySQL 저장 + ES `event_log` 색인
   b. 활성(`ENABLED`) `Workflow` 중 `triggerEventCode`가 일치하는 것이 있으면 `IngestEventUseCase`가 새 `WorkflowInstance` 생성 후 `WorkflowEngine.start(...)` 호출
   c. Redis 역색인에 `waiting:event:{eventCode}:member:{memberId}` 키로 대기 중인 인스턴스가 있으면, 각 인스턴스에 대해 `WorkflowEngine.resumeOnMatch(...)` 호출 (WAIT_FOR_EVENT의 matchConditions 평가 → 통과 시 Matched 엣지로 진행, 실패 시 대기 유지)
   d. `syntheticTimeoutForInstanceId`가 있으면: 해당 인스턴스를 `WorkflowEngine.resumeOnTimeout(instanceId)`로 직접 재개 (WAIT_FOR_EVENT는 Timeout 엣지, DELAY는 Always 엣지로 진행)
4. `triggerly-bootstrap`의 `@Scheduled(fixedDelay = 10_000)` 폴러(`WaitingInstanceTimeoutPoller`)가 `workflow_instance`에서 `status = WAITING AND waiting_until <= now()`인 행을 조회해 각각에 대해 타임아웃 합성 이벤트를 produce

## 7. 워크플로 엔진

`triggerly-application`의 `WorkflowEngine`이 `WorkflowDefinition.nodes`/`edges`를 순회하며 노드별로 다음과 같이 동작한다. 매 노드 실행마다 `WorkflowExecution` 레코드를 생성/갱신한다.

| 노드 | 동작 | 다음 엣지 |
|---|---|---|
| Trigger | 컨텍스트 구성(이벤트+회원 병합) 후 통과 | Always |
| Condition | `ConditionEvaluator.evaluate(...)` | True / False |
| Action | 데모용 모의 실행: 콘솔 로그 출력 + `WorkflowExecution.result`에 액션 내용 기록 (실제 쿠폰/푸시/알림톡 연동 없음) | Always |
| WaitForEvent | `WorkflowInstance.status = WAITING`, `waitingEventName` 설정, Redis 역색인 등록 후 실행 중단 | Matched(이벤트로 재개) / Timeout(스케줄러로 재개) |
| Delay | `WorkflowInstance.status = WAITING`, `waitingUntil = now + duration` 설정 후 실행 중단 (Redis 등록 없음, 스케줄러 폴링만) | Always(타임아웃 시점에) |
| End | `WorkflowInstance.status = COMPLETED`, `completedAt` 설정 | (없음) |

포트(인터페이스, `triggerly-application`에 정의, 각 어댑터가 구현):
- `WorkflowRepositoryPort`, `WorkflowInstanceRepositoryPort`, `WorkflowExecutionRepositoryPort` (MySQL)
- `MemberRepositoryPort` (MySQL 쓰기 + ES 읽기 겸용, 또는 MySQL/ES 분리된 두 포트)
- `EventDefinitionPort`, `AttributeDefinitionPort` (MySQL)
- `EventInstanceRepositoryPort` (MySQL+ES 저장)
- `MemberEventStatsPort` (ES count 쿼리, `ConditionEvaluator`의 `EventStatsResolver` 어댑팅)
- `EventPublisherPort` (Kafka produce)
- `WaitingIndexPort` (Redis register/remove/lookup)

## 8. 데모 시나리오 (시드 데이터)

4개의 워크플로를 시드로 등록해 전체 노드 타입과 두 종류의 CONDITION 평가(로컬 컨텍스트 / ES 집계)를 모두 시연한다.

**1) 장바구니 리마인드**
```
Trigger(CART_ADD) --Always--> WaitForEvent(PURCHASE, timeout=1m, match=memberId)
  --Matched--> End
  --Timeout--> Action(IssueCoupon) --Always--> End
```

**2) 생일 쿠폰**
```
Trigger(LOGIN) --Always--> Condition(member.birthday == today)
  --True--> Action(IssueCoupon) --Always--> End
  --False--> End
```

**3) 가입 환영 알림**
```
Trigger(SIGN_UP) --Always--> Delay(30s) --Always--> Action(SendAlimTalk) --Always--> End
```

**4) 리뷰 유도 (WAIT_FOR_EVENT + ES 집계 CONDITION 조합)**
```
Trigger(PURCHASE) --Always--> WaitForEvent(REVIEW_ADD, timeout=5m, match=memberId)
  --Matched--> Action(IssueCoupon 10%) --Always--> End
  --Timeout--> Condition(stats.eventCount:REVIEW_ADD:30d GOE 3)
      --True--> Action(SendAlimTalk "리뷰 남기면 쿠폰") --Always--> End
      --False--> End
```

데모 진입 시간을 실용적으로 확인하기 위해 시나리오 1/4의 timeout은 분 단위(1분/5분)로, 시나리오 3의 delay는 30초로 짧게 잡는다 (실제 서비스라면 시간/일 단위).

`triggerly-bootstrap`의 `DemoRunner`(`CommandLineRunner`, `demo` 프로파일에서만 활성화)가 애플리케이션 기동 시 다음을 순서대로 수행:
1. `EventDefinition`/`AttributeDefinition` 등록 (CART_ADD, PURCHASE, REVIEW_ADD, LOGIN, SIGN_UP)
2. 데모용 `Member` 1~2명 생성 (MySQL+ES)
3. 위 4개 `Workflow` 등록 (ENABLED)
4. `triggerly-sdk`를 사용해 이벤트를 순서대로 전송하며 로그로 진행 상황 출력

`scripts/demo.sh`도 별도로 제공해, curl만으로 동일한 흐름을 수동으로 재현할 수 있게 한다 (Kotlin/SDK 없이도 확인 가능).

## 9. REST API (요약)

- `POST /api/v1/events` — 이벤트 수집 (SDK가 호출)
- `POST/GET /api/v1/admin/event-definitions`
- `POST/GET /api/v1/admin/attribute-definitions`
- `POST/GET/PUT /api/v1/admin/workflows` (definitionJson 포함), `POST /api/v1/admin/workflows/{id}/enable`
- `POST/GET /api/v1/admin/members`, `GET /api/v1/admin/members/search` (ES 기반 속성 검색)
- `GET /api/v1/admin/workflow-instances/{id}` — 진행 상태 + 실행 이력 조회 (데모 확인용)

인증/인가는 데모 범위에서 제외 (14절). `POST /api/v1/events`는 테넌트별 레이트리밋 초과 시 `429 Too Many Requests` + `Retry-After` 헤더를 반환한다 (13.2절).

## 10. triggerly-sdk

독립 Gradle 모듈, Kotlin으로 작성하되 외부 의존성 최소화(Jackson만 사용, `java.net.http.HttpClient`로 REST 호출). `domain`/`application` 모듈에 의존하지 않고 자체 DTO(`SdkEventRequest` 등)를 가진다.

```kotlin
class TriggerlyClient(baseUrl: String, apiKey: String) {
  fun sendEvent(eventCode: String, externalMemberId: String?, attributes: Map<String, Any?>? = null)
}
```

## 11. 테스트 전략

- `triggerly-domain`: `ConditionEvaluator` 단위 테스트(연산자별, 컨텍스트 필드/집계 필드 각각, EventStatsResolver는 fake), 기존 스케치 파일 승격분 모델 생성 테스트
- `triggerly-application`: `WorkflowEngine` 단위 테스트 — 포트는 mockk로 대체, 6개 노드 타입 각각의 전이 검증 + 4개 데모 시나리오를 조립해 end-to-end로 전이 순서 검증
- 통합 테스트(`triggerly-bootstrap` 또는 별도 `*-it` 소스셋): Testcontainers로 MySQL/Elasticsearch/Redis/Kafka 기동 후, 이벤트 수집 API 호출 → Kafka 소비 → 엔진 실행 → DB 상태 반영까지 최소 2개 시나리오(장바구니 리마인드의 Matched/Timeout 각 1개) 검증
- `triggerly-sdk`: `TriggerlyClient`가 HTTP 요청을 올바르게 구성하는지 (WireMock 또는 MockWebServer)
- `loadtest/`의 k6 스크립트는 자동화된 테스트 스위트에 포함하지 않는다 — 수동으로 실행해 수치를 튜닝하는 도구 (13.5절)

## 12. 인프라 (docker-compose)

`triggerly-claude/docker-compose.yml`에 다음 4개 서비스를 정의: `mysql:8`, `elasticsearch:8.x`(단일 노드, security 비활성화), `redis:7`, Kafka(KRaft 모드, Zookeeper 불필요, 단일 브로커). `docker compose up -d` 한 번으로 전체 인프라 기동. `application-local.yml`이 이 포트들을 기본값으로 바라보도록 설정. 관측용 `docker-compose.observability.yml`(Prometheus + Grafana)은 선택적으로 오버레이해서 띄운다 (13.5절).

## 13. 대용량 트래픽 대응 설계

테넌트당 초당 수만 건 트래픽을 견디는 것을 목표로, 데모는 소규모로 돌리되 설정값만 바꾸면 실제로 확장 가능하도록 다음을 설계·구현한다. 로컬에서 수만 rps를 실제로 재현하는 것은 14절에 따라 범위 밖이며, 이를 위한 도구만 제공한다.

### 13.1 스레드 고갈 방지 — 수집 API

- `spring.threads.virtual.enabled=true`(Java 21 가상 스레드)로 `POST /api/v1/events`를 처리. WebFlux로 재작성하지 않고도 블로킹 I/O(JPA 검증, Kafka produce 대기 등)에서 OS 스레드가 묶이지 않게 함
- Kafka produce는 `kafkaTemplate.send(...)`의 `CompletableFuture`를 블로킹 대기하지 않고 콜백(`whenComplete`)으로 실패만 로깅 — 컨트롤러 스레드는 produce 완료를 기다리지 않고 즉시 202 반환
- 요청 바디 크기 제한(`server.tomcat.max-swallow-size` 등)으로 비정상적으로 큰 `attributes` 페이로드로 인한 메모리 급증 방지

### 13.2 백프레셔 — 테넌트별 레이트 리밋

- Redis 기반 토큰 버킷(Lua 스크립트 또는 Bucket4j-Redis)으로 `tenantId`별 초당 요청 수를 제한. 여러 `adapter-in-web` 인스턴스로 수평 확장해도 Redis에 상태가 공유되므로 정확히 동작
- 기본값은 `application.yml`의 `triggerly.ratelimit.default-rps`(예: 2000)로 설정, 테넌트별 override는 in-memory 설정 맵으로 데모 수준에서 지원 (DB 테이블화는 14절의 향후 과제)
- 초과 시 `429 Too Many Requests` + `Retry-After` 헤더 반환 — 무한정 큐잉하지 않고 클라이언트(SDK)가 알 수 있게 명시적으로 거부

### 13.3 Kafka 파티셔닝 & 컨슈머 스케일링

- `triggerly.events.raw` 토픽 파티션 수를 설정값으로 노출(`triggerly.kafka.raw-events-topic.partitions`, 기본 32) — 파티션 수 = 최대 병렬 컨슈머 스레드 수의 상한
- 파티션 키는 `tenantId:memberId` 유지: 같은 회원의 이벤트 순서는 보장하면서, 회원 수가 많은 테넌트는 자연히 여러 파티션에 분산되어 특정 테넌트가 파티션 하나를 독점(hot partition)하는 걸 방지
- `@KafkaListener(concurrency = "${triggerly.kafka.consumer.concurrency}")`로 컨슈머 스레드 수를 고정 설정값으로 제한(기본 8) — 스레드가 무한정 늘어나지 않고, 인스턴스를 늘리면 컨슈머 그룹이 자동 재분배되어 수평 확장됨
- 배치 컨슈밍(`max.poll.records` 기본 500)으로 MySQL/ES에 벌크로 저장 — 처리량은 올리면서 한 번에 메모리에 올라오는 메시지 수는 상한선을 둠

### 13.4 OOM 방어

- 회원 ES 동기화용 `@Async` 실행기: `ThreadPoolTaskExecutor`(core/max/queueCapacity 전부 설정값, 예: 4/16/1000) + `CallerRunsPolicy` — 큐가 가득 차면 새 작업을 무한히 쌓지 않고 호출 스레드가 직접 처리하도록 해 자연스럽게 백프레셔가 걸림
- ES 색인은 `BulkProcessor`/`BulkIngester` 사용 — bulk 크기(문서 수/바이트 수)와 flush 주기를 설정값으로 제한해 대기 중인 벌크 버퍼가 무한정 커지지 않게 함
- 모든 풀/버퍼/배치 크기는 `application.yml`에 노출된 설정값 — 코드 수정 없이 숫자만 바꿔가며 튜닝 가능

### 13.5 관측 & 부하테스트 도구

- Spring Boot Actuator + Micrometer Prometheus 레지스트리 추가(`/actuator/prometheus`) — JVM 스레드/힙, HTTP 요청 지연, Kafka 컨슈머 랙 관찰 가능
- 기본 `docker-compose.yml`은 가볍게 유지하고, 선택적으로 `docker-compose.observability.yml`(Prometheus + Grafana)을 오버레이로 제공(`docker compose -f docker-compose.yml -f docker-compose.observability.yml up`)
- `loadtest/ingest.js` — k6 스크립트. 환경변수로 목표 RPS, VU 수, 지속시간, tenantId/memberId 분포를 조절 가능. `loadtest/README.md`에 100 → 1,000 → 10,000 rps로 단계적으로 올려보는 예시 명령어 제공. 실제 수만 rps 검증은 로컬 리소스 한계상 별도 환경(EC2 등)에서 진행하는 걸 전제로 함

## 14. 범위 제외 (Out of scope)

- 인증/인가, 멀티테넌시 격리(테넌트 ID는 필드로만 존재, 접근 제어 없음)
- 실제 쿠폰/푸시/알림톡 외부 연동 (전부 로그 출력으로 모의 처리)
- 워크플로 버전 관리/무중단 배포, A/B 분기
- 테넌트별 레이트리밋의 DB 기반 동적 설정 UI (데모는 설정 파일 override로 대체)
- 실제 수만 rps 부하테스트 실행/튜닝 (도구는 제공하되 실행은 사용자가 로컬 또는 별도 환경에서 진행)
- 관리자 프론트엔드 UI (REST API + Swagger/OpenAPI 문서만 제공)
- `event_instance` 외 다른 테이블의 보관주기 정책
