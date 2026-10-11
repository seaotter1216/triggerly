package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkflowInstanceQueryUseCaseTest {

  private val instancePort = mockk<WorkflowInstanceRepositoryPort>()
  private val executionPort = mockk<WorkflowExecutionRepositoryPort>()
  private val useCase = WorkflowInstanceQueryUseCase(instancePort, executionPort)

  private fun instance(id: String, workflowId: String) = WorkflowInstance(
    id = id, workflowId = workflowId, tenantId = "t1", triggerEventCode = "LOGIN",
    version = 1, status = WorkflowInstanceStatus.RUNNING,
  )

  @Test
  fun `workflowId가 없으면 테넌트의 인스턴스를 전부 반환한다`() {
    val instances = listOf(instance("i1", "wf-1"), instance("i2", "wf-2"))
    every { instancePort.findByTenant("t1", 50) } returns instances

    val result = useCase.list("t1")

    assertEquals(instances, result)
  }

  @Test
  fun `workflowId가 주어지면 해당 워크플로의 인스턴스만 필터링한다`() {
    val instances = listOf(instance("i1", "wf-1"), instance("i2", "wf-2"))
    every { instancePort.findByTenant("t1", 50) } returns instances

    val result = useCase.list("t1", workflowId = "wf-2")

    assertEquals(listOf(instances[1]), result)
  }
}
