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
  fun `WorkflowDefinition은 모든 노드 및 엣지 타입을 포함해 JSON으로 직렬화 후 복원할 수 있다`() {
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
