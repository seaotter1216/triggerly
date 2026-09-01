package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface WorkflowJpaRepository : JpaRepository<WorkflowEntity, String> {
  fun findByIdAndTenantId(id: String, tenantId: String): WorkflowEntity?
  fun findAllByTenantId(tenantId: String): List<WorkflowEntity>
  fun findAllByTenantIdAndTriggerEventCodeAndStatus(tenantId: String, triggerEventCode: String, status: String): List<WorkflowEntity>
}
