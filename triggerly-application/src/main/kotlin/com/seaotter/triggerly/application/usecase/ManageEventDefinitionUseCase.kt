package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.stereotype.Service

@Service
class ManageEventDefinitionUseCase(private val port: EventDefinitionPort) {
  fun register(tenantId: String, code: String, displayName: String): EventDefinition =
    port.save(EventDefinition(tenantId = tenantId, code = code, displayName = displayName))

  fun list(tenantId: String): List<EventDefinition> = port.findAll(tenantId)
}
