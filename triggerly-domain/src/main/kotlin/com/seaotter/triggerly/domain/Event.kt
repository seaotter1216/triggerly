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
