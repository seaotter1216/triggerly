package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface MemberJpaRepository : JpaRepository<MemberEntity, String> {
  fun findByTenantIdAndExternalMemberId(tenantId: String, externalMemberId: String): MemberEntity?
  fun findByTenantIdAndExternalMemberIdIn(tenantId: String, externalMemberIds: Collection<String>): List<MemberEntity>
}
