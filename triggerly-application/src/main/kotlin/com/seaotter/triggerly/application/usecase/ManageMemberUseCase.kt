package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.MemberCommandPort
import com.seaotter.triggerly.application.port.MemberQueryPort
import com.seaotter.triggerly.domain.Member
import org.springframework.stereotype.Service

@Service
class ManageMemberUseCase(
  private val memberCommandPort: MemberCommandPort,
  private val memberQueryPort: MemberQueryPort,
) {
  fun register(member: Member): Member = memberCommandPort.save(member)
  fun search(tenantId: String, criteria: Map<String, Any?>, limit: Int = 50): List<Member> =
    memberQueryPort.search(tenantId, criteria, limit)
}
