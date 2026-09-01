package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowStatus
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ManageWorkflowUseCase(private val port: WorkflowRepositoryPort) {
  fun create(workflow: Workflow): Workflow = port.save(workflow)
  fun list(tenantId: String): List<Workflow> = port.findAll(tenantId)
  fun get(tenantId: String, id: String): Workflow? = port.findById(tenantId, id)

  fun enable(tenantId: String, id: String): Workflow {
    val workflow = port.findById(tenantId, id) ?: error("workflow not found: $id")
    workflow.status = WorkflowStatus.ENABLED
    workflow.lastUpdatedAt = LocalDateTime.now()
    return port.save(workflow)
  }
}
