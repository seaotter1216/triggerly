package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface EventDefinitionJpaRepository : JpaRepository<EventDefinitionEntity, String> {
  fun findByTenantIdAndCode(tenantId: String, code: String): EventDefinitionEntity?
  fun findAllByTenantId(tenantId: String): List<EventDefinitionEntity>
}
