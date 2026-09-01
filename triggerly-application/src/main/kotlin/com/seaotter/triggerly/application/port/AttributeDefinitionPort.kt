package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.AttributeDefinition

interface AttributeDefinitionPort {
  fun save(definition: AttributeDefinition): AttributeDefinition
  fun findAll(tenantId: String): List<AttributeDefinition>
}
