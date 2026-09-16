package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.ConditionExpression
import com.seaotter.triggerly.domain.ConditionOperator
import com.seaotter.triggerly.domain.Edge
import com.seaotter.triggerly.domain.EdgeRoute
import com.seaotter.triggerly.domain.Node
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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

    val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
    val useCase = ManageWorkflowUseCase(port, cacheInvalidationPort)
    val result = useCase.enable("t1", "wf-1")

    assertEquals(WorkflowStatus.ENABLED, result.status)
    verify { port.save(workflow) }
  }

  @Test
  fun `create는 순환 워크플로 정의에 대해 예외를 던진다`() {
    val port = mockk<WorkflowRepositoryPort>()
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
    val workflow = Workflow(
      id = "wf-2", tenantId = "t1", triggerEventCode = "LOGIN",
      definitionJson = definition,
      status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )

    val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
    val useCase = ManageWorkflowUseCase(port, cacheInvalidationPort)
    assertFailsWith<IllegalArgumentException> { useCase.create(workflow) }
  }

  @Test
  fun `create는 존재하지 않는 노드를 가리키는 엣지에 대해 예외를 던진다`() {
    val port = mockk<WorkflowRepositoryPort>()
    val definition = WorkflowDefinition(
      trigger = "LOGIN",
      nodes = listOf(
        Node.Trigger("n1", "LOGIN"),
        Node.End("n2"),
      ),
      edges = listOf(
        Edge("n1", "does-not-exist", EdgeRoute.Always),
      ),
    )
    val workflow = Workflow(
      id = "wf-3", tenantId = "t1", triggerEventCode = "LOGIN",
      definitionJson = definition,
      status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )

    val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
    val useCase = ManageWorkflowUseCase(port, cacheInvalidationPort)
    assertFailsWith<IllegalArgumentException> { useCase.create(workflow) }
  }

  @Test
  fun `enable은 성공 후 WORKFLOW 캐시 무효화를 발행한다`() {
    val port = mockk<WorkflowRepositoryPort>()
    val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
    val workflow = Workflow(
      id = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
      definitionJson = WorkflowDefinition("LOGIN", emptyList(), emptyList()),
      status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )
    every { port.findById("t1", "wf-1") } returns workflow
    every { port.save(any()) } answers { firstArg() }

    ManageWorkflowUseCase(port, cacheInvalidationPort).enable("t1", "wf-1")

    verify(exactly = 1) { cacheInvalidationPort.publish(CacheInvalidationTopic.WORKFLOW, "t1:wf-1") }
  }
}
