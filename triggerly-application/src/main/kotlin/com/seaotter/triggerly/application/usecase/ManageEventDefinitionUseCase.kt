package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.stereotype.Service

@Service
class ManageEventDefinitionUseCase(
  private val port: EventDefinitionPort,
  private val cacheInvalidationPort: CacheInvalidationPort,
) {
  fun register(tenantId: String, code: String, displayName: String): EventDefinition {
    val saved = port.save(EventDefinition(tenantId = tenantId, code = code, displayName = displayName))
    cacheInvalidationPort.publish(CacheInvalidationTopic.EVENT_DEFINITION, "$tenantId:$code")
    return saved
  }

  fun list(tenantId: String): List<EventDefinition> = port.findAll(tenantId)
}
