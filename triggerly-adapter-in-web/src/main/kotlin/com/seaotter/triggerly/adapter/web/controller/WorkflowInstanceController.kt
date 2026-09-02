package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.application.usecase.WorkflowInstanceQueryUseCase
import com.seaotter.triggerly.application.usecase.WorkflowInstanceView
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/admin/workflow-instances")
class WorkflowInstanceController(private val useCase: WorkflowInstanceQueryUseCase) {

  @GetMapping("/{id}")
  fun get(@PathVariable id: String): ResponseEntity<WorkflowInstanceView> =
    useCase.get(id)?.let { ResponseEntity.ok(it) } ?: ResponseEntity.notFound().build()
}
