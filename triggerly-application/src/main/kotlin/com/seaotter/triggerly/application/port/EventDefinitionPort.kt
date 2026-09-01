package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.EventDefinition

interface EventDefinitionPort {
  fun save(definition: EventDefinition): EventDefinition
  fun findByTenantAndCode(tenantId: String, code: String): EventDefinition?
  fun findAll(tenantId: String): List<EventDefinition>
}
