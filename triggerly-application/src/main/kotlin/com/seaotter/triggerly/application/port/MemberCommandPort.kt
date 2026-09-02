package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Member

interface MemberCommandPort {
  fun save(member: Member): Member
  fun findById(id: String): Member?
  fun findByExternalId(tenantId: String, externalMemberId: String): Member?

  // Kafka 배치 컨슈머(IngestEventUseCase.handleBatch)가 N번의 개별 조회/저장 대신
  // tenantId 단위로 IN절 조회 1회 + 일괄 저장 1회로 묶기 위한 벌크 메서드.
  fun findByExternalIds(tenantId: String, externalMemberIds: Collection<String>): List<Member>
  fun saveAll(members: Collection<Member>): List<Member>
}
