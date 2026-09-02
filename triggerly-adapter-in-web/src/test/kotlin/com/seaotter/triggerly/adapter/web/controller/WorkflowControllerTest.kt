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
