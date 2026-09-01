package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.stereotype.Component

@Component
class EventDefinitionPersistenceAdapter(
  private val repository: EventDefinitionJpaRepository,
) : EventDefinitionPort {

  override fun save(definition: EventDefinition): EventDefinition {
    val entity = EventDefinitionEntity(
      id = definition.id,
      tenantId = definition.tenantId,
      code = definition.code,
      displayName = definition.displayName,
      createdAt = definition.createdAt,
    )
    return repository.save(entity).toDomain()
  }

  override fun findByTenantAndCode(tenantId: String, code: String): EventDefinition? =
    repository.findByTenantIdAndCode(tenantId, code)?.toDomain()

  override fun findAll(tenantId: String): List<EventDefinition> =
    repository.findAllByTenantId(tenantId).map { it.toDomain() }

  private fun EventDefinitionEntity.toDomain() = EventDefinition(tenantId, code, displayName, createdAt, id)
}
