package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.WorkflowExecution

interface WorkflowExecutionRepositoryPort {
  fun save(execution: WorkflowExecution): WorkflowExecution
  fun findByInstanceId(instanceId: String): List<WorkflowExecution>
  fun findRunning(instanceId: String, nodeId: String): WorkflowExecution?
}
