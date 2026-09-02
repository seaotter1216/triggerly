package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.CreateWorkflowRequest
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.domain.Workflow
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/workflows")
class WorkflowController(private val useCase: ManageWorkflowUseCase) {

  @PostMapping
  fun create(@RequestBody request: CreateWorkflowRequest): Workflow = useCase.create(request.toDomain())

  @GetMapping
  fun list(@RequestParam tenantId: String): List<Workflow> = useCase.list(tenantId)

  @GetMapping("/{id}")
  fun get(@RequestParam tenantId: String, @PathVariable id: String): ResponseEntity<Workflow> =
    useCase.get(tenantId, id)?.let { ResponseEntity.ok(it) } ?: ResponseEntity.notFound().build()

  @PostMapping("/{id}/enable")
  fun enable(@RequestParam tenantId: String, @PathVariable id: String): Workflow = useCase.enable(tenantId, id)
}
