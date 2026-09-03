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

fun ActionDefinition.describe(): String = when (this) {
  is ActionDefinition.IssueCoupon -> "쿠폰 발급: couponId=$couponId"
  is ActionDefinition.SendPush -> "푸시 발송: templateId=$templateId"
  is ActionDefinition.SendAlimTalk -> "알림톡 발송: templateId=$templateId"
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
