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
