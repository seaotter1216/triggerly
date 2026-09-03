package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.MemberContext
import java.time.LocalDateTime
import java.util.UUID

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
  // 재시도/재배달 되어도 값이 변하지 않는 멱등 키. publish 시점(컨트롤러/폴러)에서 딱 한 번만 생성되고
  // 이 객체가 그대로 JSON 직렬화되어 Kafka에 저장되므로, 같은 레코드가 다시 배달돼도(consumer retry)
  // 역직렬화하면 항상 같은 값이 나온다. IngestEventUseCase.handleBatch가 이 값으로 "이미 처리된 이벤트"를
  // 구분해 액션(쿠폰 발급 등)이 재시도 때 중복 실행되는 것을 막는다.
  val eventId: String = UUID.randomUUID().toString(),
)

fun interface EventPublisherPort {
  fun publish(message: RawEventMessage)
}
