package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterAttributeDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageAttributeDefinitionUseCase
import com.seaotter.triggerly.domain.AttributeDefinition
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/attribute-definitions")
class AttributeDefinitionController(private val useCase: ManageAttributeDefinitionUseCase) {

  @PostMapping
  fun register(@RequestBody request: RegisterAttributeDefinitionRequest): AttributeDefinition =
    useCase.register(request.tenantId, request.eventDefinitionId, request.key, request.displayName, request.type, request.filterable)

  @GetMapping
  fun list(@RequestParam tenantId: String): List<AttributeDefinition> = useCase.list(tenantId)
}
