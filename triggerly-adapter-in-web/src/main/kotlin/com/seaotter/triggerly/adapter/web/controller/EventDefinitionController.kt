package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterEventDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.domain.EventDefinition
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/event-definitions")
class EventDefinitionController(private val useCase: ManageEventDefinitionUseCase) {

  @PostMapping
  fun register(@RequestBody request: RegisterEventDefinitionRequest): EventDefinition =
    useCase.register(request.tenantId, request.code, request.displayName)

  @GetMapping
  fun list(@RequestParam tenantId: String): List<EventDefinition> = useCase.list(tenantId)
}
