package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.application.usecase.WorkflowInstanceQueryUseCase
import com.seaotter.triggerly.application.usecase.WorkflowInstanceView
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import org.springframework.http.HttpStatus
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkflowInstanceControllerTest {

  private val useCase = mockk<WorkflowInstanceQueryUseCase>()
  private val controller = WorkflowInstanceController(useCase)

  @Test
  fun `존재하는 인스턴스는 200과 함께 반환한다`() {
    val instance = WorkflowInstance(
      id = "i1", workflowId = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
      version = 1, status = WorkflowInstanceStatus.COMPLETED,
    )
    every { useCase.get("i1") } returns WorkflowInstanceView(instance, emptyList())

    val response = controller.get("i1")

    assertEquals(HttpStatus.OK, response.statusCode)
  }

  @Test
  fun `없는 인스턴스는 404를 반환한다`() {
    every { useCase.get("unknown") } returns null

    assertEquals(HttpStatus.NOT_FOUND, controller.get("unknown").statusCode)
  }

  @Test
  fun `목록 조회는 tenantId와 workflowId를 그대로 유스케이스에 위임한다`() {
    val instance = WorkflowInstance(
      id = "i1", workflowId = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
      version = 1, status = WorkflowInstanceStatus.RUNNING,
    )
    every { useCase.list("t1", "wf-1") } returns listOf(instance)

    val result = controller.list("t1", "wf-1")

    assertEquals(listOf(instance), result)
  }
}
