package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Workflow

interface WorkflowRepositoryPort {
  fun save(workflow: Workflow): Workflow
  fun findById(tenantId: String, id: String): Workflow?
  fun findAll(tenantId: String): List<Workflow>
  fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow>
}
