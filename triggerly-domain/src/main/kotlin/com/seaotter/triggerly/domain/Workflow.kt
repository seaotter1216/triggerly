package com.seaotter.triggerly.domain

import com.fasterxml.jackson.annotation.JsonIgnore
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

// 정의된 워크플로가 실행될 때마다 생기는 인스턴스
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
) {
  fun markError(nodeId: String) {
    status = WorkflowInstanceStatus.ERROR
    currentNodeId = nodeId
  }

  fun markWaitingForEvent(node: Node.WaitForEvent, now: LocalDateTime = LocalDateTime.now()) {
    status = WorkflowInstanceStatus.WAITING
    currentNodeId = node.id
    waitingEventName = node.event.eventCode
    waitingUntil = node.event.timeout?.let { now.plusNanos(it.toDuration().inWholeNanoseconds) }
  }

  fun markWaitingForDelay(node: Node.Delay, now: LocalDateTime = LocalDateTime.now()) {
    status = WorkflowInstanceStatus.WAITING
    currentNodeId = node.id
    waitingUntil = now.plusNanos(node.duration.toDuration().inWholeNanoseconds)
  }

  fun markCompleted(nodeId: String, now: LocalDateTime = LocalDateTime.now()) {
    status = WorkflowInstanceStatus.COMPLETED
    currentNodeId = nodeId
    completedAt = now
  }
}

enum class WorkflowInstanceStatus { RUNNING, WAITING, COMPLETED, EXPIRED, ERROR }

// 워크플로인스턴스 하위 노드들에 대한 데이터
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
) {
  // runFrom의 while 루프가 정상적인(순환 없는) 워크플로에서 밟을 수 있는 최대 스텝 수보다 넉넉한 여유값.
  // ManageWorkflowUseCase.validate()가 생성 시점에 순환을 이미 거부하므로 정상 워크플로는 각 노드를
  // 최대 1번만 방문하지만, 검증을 뚫고 들어온 비정상 순환에 대비한 2차 방어선이다.
  // @JsonIgnore: 파생 프로퍼티라 직렬화 대상에서 빠져야 한다 - 안 그러면 저장 시 함께 직렬화됐다가
  // 복원 시 생성자에 없는 프로퍼티라 UnrecognizedPropertyException이 발생한다(Node.nodeType과 동일 이유).
  @get:JsonIgnore
  val maxExecutionSteps: Int get() = nodes.size * 4

  fun nodeById(id: String): Node? = nodes.firstOrNull { it.id == id }

  fun nextNodeId(from: String, route: EdgeRoute): String =
    edges.firstOrNull { it.from == from && it.route == route }?.to
      ?: error("노드 $from 에서 $route 로 가는 엣지가 없습니다")
}

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

  // JsonTypeInfo가 이미 "type" 프로퍼티를 직렬화 메타데이터로 쓰고 있어(클래스 -> TRIGGER/CONDITION/...
  // 이름 매핑) 이름 충돌을 피하려고 "nodeType"으로 둔다. @JsonIgnore로 이 파생 프로퍼티 자체가
  // JSON 직렬화/역직렬화 대상에 포함되지 않게 한다 - 안 그러면 저장 시 nodeType 필드가 함께
  // 직렬화됐다가, 복원 시 생성자에 없는 프로퍼티라 UnrecognizedPropertyException이 발생한다.
  @get:JsonIgnore
  val nodeType: NodeType

  data class Trigger(override val id: String, val eventCode: String) : Node {
    @get:JsonIgnore override val nodeType get() = NodeType.TRIGGER
  }
  data class Condition(override val id: String, val condition: ConditionExpression) : Node {
    @get:JsonIgnore override val nodeType get() = NodeType.CONDITION
  }
  data class Action(override val id: String, val action: ActionDefinition) : Node {
    @get:JsonIgnore override val nodeType get() = NodeType.ACTION
  }
  data class WaitForEvent(override val id: String, val event: WaitEventDefinition) : Node {
    @get:JsonIgnore override val nodeType get() = NodeType.WAIT_FOR_EVENT
  }
  data class Delay(override val id: String, val duration: DurationDto) : Node {
    @get:JsonIgnore override val nodeType get() = NodeType.DELAY
  }
  data class End(override val id: String) : Node {
    @get:JsonIgnore override val nodeType get() = NodeType.END
  }
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
