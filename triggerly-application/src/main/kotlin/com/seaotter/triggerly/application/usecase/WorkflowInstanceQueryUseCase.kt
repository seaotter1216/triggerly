package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowInstance
import org.springframework.stereotype.Service

data class WorkflowInstanceView(val instance: WorkflowInstance, val executions: List<WorkflowExecution>)

@Service
class WorkflowInstanceQueryUseCase(
  private val instancePort: WorkflowInstanceRepositoryPort,
  private val executionPort: WorkflowExecutionRepositoryPort,
) {
  fun get(id: String): WorkflowInstanceView? {
    val instance = instancePort.findById(id) ?: return null
    return WorkflowInstanceView(instance, executionPort.findByInstanceId(id))
  }
}
