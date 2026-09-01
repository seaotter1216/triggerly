package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.application.port.AttributeDefinitionPort
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import org.springframework.stereotype.Component

@Component
class AttributeDefinitionPersistenceAdapter(
  private val repository: AttributeDefinitionJpaRepository,
) : AttributeDefinitionPort {

  override fun save(definition: AttributeDefinition): AttributeDefinition {
    val entity = AttributeDefinitionEntity(
      id = definition.id,
      tenantId = definition.tenantId,
      eventDefinitionId = definition.eventDefinitionId,
      attrKey = definition.key,
      displayName = definition.displayName,
      attrType = definition.type.name,
      filterable = definition.filterable,
      createdAt = definition.createdAt,
    )
    return repository.save(entity).toDomain()
  }

  override fun findAll(tenantId: String): List<AttributeDefinition> =
    repository.findAllByTenantId(tenantId).map { it.toDomain() }

  private fun AttributeDefinitionEntity.toDomain() = AttributeDefinition(
    id = id, tenantId = tenantId, eventDefinitionId = eventDefinitionId, key = attrKey,
    displayName = displayName, type = AttributeType.valueOf(attrType), filterable = filterable, createdAt = createdAt,
  )
}
