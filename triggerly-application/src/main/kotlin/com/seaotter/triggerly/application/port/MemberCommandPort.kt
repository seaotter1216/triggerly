package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Member

interface MemberCommandPort {
  fun save(member: Member): Member
  fun findById(id: String): Member?
  fun findByExternalId(tenantId: String, externalMemberId: String): Member?
}
