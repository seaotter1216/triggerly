package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.AttributeDefinitionPort
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ManageAttributeDefinitionUseCase(private val port: AttributeDefinitionPort) {
  fun register(
    tenantId: String,
    eventDefinitionId: String?,
    key: String,
    displayName: String,
    type: AttributeType,
    filterable: Boolean,
  ): AttributeDefinition = port.save(
    AttributeDefinition(
      id = UUID.randomUUID().toString(),
      tenantId = tenantId,
      eventDefinitionId = eventDefinitionId,
      key = key,
      displayName = displayName,
      type = type,
      filterable = filterable,
    ),
  )

  fun list(tenantId: String): List<AttributeDefinition> = port.findAll(tenantId)
}
