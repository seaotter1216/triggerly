package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
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

    val useCase = ManageWorkflowUseCase(port)
    val result = useCase.enable("t1", "wf-1")

    assertEquals(WorkflowStatus.ENABLED, result.status)
    verify { port.save(workflow) }
  }
}
