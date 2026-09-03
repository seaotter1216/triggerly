package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.ActionDispatchPort
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

private class FakeActionDispatchPort : ActionDispatchPort {
  val published = mutableListOf<ActionDispatchMessage>()
  override fun publish(message: ActionDispatchMessage) { published += message }
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

  private fun engine(counts: Map<String, Long> = emptyMap()): Pair<WorkflowEngine, FakeActionDispatchPort> {
    val dispatchPort = FakeActionDispatchPort()
    val engine = WorkflowEngine(
      FakeWorkflowInstanceRepository(),
      FakeWorkflowExecutionRepository(),
      FakeWaitingIndexPort(),
      FakeMemberEventStatsPort(counts),
      dispatchPort,
    )
    return engine to dispatchPort
  }

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
    val (engine, _) = engine()
    val instance = engine.start(
      workflow(definition, "LOGIN"),
      tenantId = "tenant-1",
      memberId = "m1",
      context = mapOf("member.isBirthdayToday" to true),
      eventId = "evt-1",
    )
    assertEquals(WorkflowInstanceStatus.COMPLETED, instance.status)
    assertEquals("n4", instance.currentNodeId)
  }

  @Test
  fun `동일한 eventId로 다시 start하면(재시도) 이미 있는 인스턴스를 그대로 반환하고 재실행하지 않는다`() {
    val definition = WorkflowDefinition(
      trigger = "LOGIN",
      nodes = listOf(
        Node.Trigger("n1", "LOGIN"),
        Node.Action("n2", ActionDefinition.IssueCoupon("BIRTHDAY10")),
        Node.End("n3"),
      ),
      edges = listOf(
        Edge("n1", "n2", EdgeRoute.Always),
        Edge("n2", "n3", EdgeRoute.Always),
      ),
    )
    val (engine, dispatchPort) = engine()
    val wf = workflow(definition, "LOGIN")
    val first = engine.start(wf, "tenant-1", "m1", emptyMap(), eventId = "evt-retry")
    val retried = engine.start(wf, "tenant-1", "m1", emptyMap(), eventId = "evt-retry")

    assertEquals(first.id, retried.id)
    assertEquals(WorkflowInstanceStatus.COMPLETED, retried.status)
    // 재시도로 start()가 두 번 호출돼도 Action 노드는 다시 안 도니까, ActionDispatchMessage도 한 번만 나가야 한다
    // (그래야 dispatch 토픽 자체의 재배달 문제와 별개로, 애초에 중복 발행조차 안 생긴다).
    assertEquals(1, dispatchPort.published.size)
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
      FakeActionDispatchPort(),
    )
    val instance = engine.start(workflow(definition, "CART_ADD"), "tenant-1", "m1", emptyMap(), eventId = "evt-2")
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
      FakeActionDispatchPort(),
    )
    val wf = workflow(definition, "PURCHASE")
    val instance = engine.start(wf, "tenant-1", "m1", emptyMap(), eventId = "evt-3")
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
    val (engine, _) = engine()
    val wf = workflow(definition, "SIGN_UP")
    val instance = engine.start(wf, "tenant-1", "m1", emptyMap(), eventId = "evt-4")
    assertEquals(WorkflowInstanceStatus.WAITING, instance.status)
    assertNull(instance.waitingEventName)

    val resumed = engine.resumeOnTimeout(instance, wf)
    assertEquals(WorkflowInstanceStatus.COMPLETED, resumed.status)
    assertEquals("n4", resumed.currentNodeId)
  }

  @Test
  fun `순환(cycle) 워크플로 정의는 스텝 상한을 넘으면 ERROR 상태로 종료된다`() {
    val definition = WorkflowDefinition(
      trigger = "LOGIN",
      nodes = listOf(
        Node.Trigger("n1", "LOGIN"),
        Node.Condition("n2", ConditionExpression.Predicate("always", ConditionOperator.EQ, true)),
        Node.Condition("n3", ConditionExpression.Predicate("always", ConditionOperator.EQ, true)),
      ),
      edges = listOf(
        Edge("n1", "n2", EdgeRoute.Always),
        Edge("n2", "n3", EdgeRoute.True),
        Edge("n3", "n2", EdgeRoute.True),
      ),
    )
    val (engine, _) = engine()
    val instance = engine.start(
      workflow(definition, "LOGIN"),
      tenantId = "tenant-1",
      memberId = "m1",
      context = mapOf("always" to true),
      eventId = "evt-5",
    )
    assertEquals(WorkflowInstanceStatus.ERROR, instance.status)
  }
}
