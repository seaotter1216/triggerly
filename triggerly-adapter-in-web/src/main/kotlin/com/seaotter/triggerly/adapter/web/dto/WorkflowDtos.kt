package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import java.time.LocalDateTime

data class CreateWorkflowRequest(
  val id: String,
  val tenantId: String,
  val name: String?,
  val triggerEventCode: String,
  val definitionJson: WorkflowDefinition,
) {
  fun toDomain() = Workflow(
    id = id, tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
    definitionJson = definitionJson, status = WorkflowStatus.DRAFT,
    createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )
}
