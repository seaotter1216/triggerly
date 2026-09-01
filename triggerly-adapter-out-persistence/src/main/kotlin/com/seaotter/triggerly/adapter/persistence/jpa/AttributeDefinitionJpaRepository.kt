package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface AttributeDefinitionJpaRepository : JpaRepository<AttributeDefinitionEntity, String> {
  fun findAllByTenantId(tenantId: String): List<AttributeDefinitionEntity>
}
