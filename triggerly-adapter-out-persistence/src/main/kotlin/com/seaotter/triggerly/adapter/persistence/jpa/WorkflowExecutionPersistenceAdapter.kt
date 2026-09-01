package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.domain.NodeType
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowExecutionStatus
import org.springframework.stereotype.Component

@Component
class WorkflowExecutionPersistenceAdapter(
  private val repository: WorkflowExecutionJpaRepository,
) : WorkflowExecutionRepositoryPort {

  override fun save(execution: WorkflowExecution): WorkflowExecution = repository.save(execution.toEntity()).toDomain()

  override fun findByInstanceId(instanceId: String): List<WorkflowExecution> =
    repository.findAllByWorkflowInstanceId(instanceId).map { it.toDomain() }

  override fun findRunning(instanceId: String, nodeId: String): WorkflowExecution? =
    repository.findFirstByWorkflowInstanceIdAndNodeIdAndStatus(instanceId, nodeId, WorkflowExecutionStatus.RUNNING.name)?.toDomain()

  private fun WorkflowExecution.toEntity() = WorkflowExecutionEntity(
    id = id, workflowInstanceId = workflowInstanceId, nodeId = nodeId, nodeType = nodeType.name,
    status = status.name, startedAt = startedAt, completedAt = completedAt, result = result, errorCode = errorCode,
  )

  private fun WorkflowExecutionEntity.toDomain() = WorkflowExecution(
    id = id, workflowInstanceId = workflowInstanceId, nodeId = nodeId, nodeType = NodeType.valueOf(nodeType),
    status = WorkflowExecutionStatus.valueOf(status), startedAt = startedAt, completedAt = completedAt,
    result = result, errorCode = errorCode,
  )
}
