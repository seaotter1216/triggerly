package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowStatus
import org.springframework.stereotype.Component

@Component
class WorkflowPersistenceAdapter(private val repository: WorkflowJpaRepository) : WorkflowRepositoryPort {

  override fun save(workflow: Workflow): Workflow = repository.save(workflow.toEntity()).toDomain()

  override fun findById(tenantId: String, id: String): Workflow? =
    repository.findByIdAndTenantId(id, tenantId)?.toDomain()

  override fun findAll(tenantId: String): List<Workflow> = repository.findAllByTenantId(tenantId).map { it.toDomain() }

  override fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow> =
    repository.findAllByTenantIdAndTriggerEventCodeAndStatus(tenantId, eventCode, WorkflowStatus.ENABLED.name).map { it.toDomain() }

  override fun findDistinctTenantIds(): Set<String> = repository.findDistinctTenantIds()

  private fun Workflow.toEntity() = WorkflowEntity(
    id = id, tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
    definitionJson = definitionJson, version = version, status = status.name,
    createdAt = createdAt, lastUpdatedAt = lastUpdatedAt,
  )

  private fun WorkflowEntity.toDomain() = Workflow(
    id = id, tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
    definitionJson = definitionJson, version = version, status = WorkflowStatus.valueOf(status),
    createdAt = createdAt, lastUpdatedAt = lastUpdatedAt,
  )
}
