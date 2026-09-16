package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import org.springframework.data.domain.PageRequest
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class WorkflowInstancePersistenceAdapter(
  private val repository: WorkflowInstanceJpaRepository,
) : WorkflowInstanceRepositoryPort {

  override fun save(instance: WorkflowInstance): WorkflowInstance = repository.save(instance.toEntity()).toDomain()

  override fun findById(id: String): WorkflowInstance? = repository.findById(id).orElse(null)?.toDomain()

  override fun findWaitingExpired(now: LocalDateTime, limit: Int): List<WorkflowInstance> =
    repository.findAllByStatusAndWaitingUntilLessThanEqual(WorkflowInstanceStatus.WAITING.name, now, PageRequest.of(0, limit))
      .map { it.toDomain() }

  override fun findAllById(ids: Collection<String>): List<WorkflowInstance> {
    if (ids.isEmpty()) return emptyList()
    return repository.findAllById(ids).map { it.toDomain() }
  }

  override fun findWaitingExpiredByTenant(tenantId: String, now: LocalDateTime, limit: Int): List<WorkflowInstance> =
    repository.findAllByTenantIdAndStatusAndWaitingUntilLessThanEqualOrderByWaitingUntilAsc(
      tenantId, WorkflowInstanceStatus.WAITING.name, now, PageRequest.of(0, limit),
    ).map { it.toDomain() }

  private fun WorkflowInstance.toEntity() = WorkflowInstanceEntity(
    id = id, workflowId = workflowId, tenantId = tenantId, triggerEventCode = triggerEventCode, version = version,
    memberId = memberId, status = status.name, currentNodeId = currentNodeId, waitingEventName = waitingEventName,
    waitingUntil = waitingUntil, startedAt = startedAt, completedAt = completedAt,
  )

  private fun WorkflowInstanceEntity.toDomain() = WorkflowInstance(
    id = id, workflowId = workflowId, tenantId = tenantId, triggerEventCode = triggerEventCode, version = version,
    memberId = memberId, status = WorkflowInstanceStatus.valueOf(status), currentNodeId = currentNodeId,
    waitingEventName = waitingEventName, waitingUntil = waitingUntil, startedAt = startedAt, completedAt = completedAt,
  )
}
