package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Member

interface MemberQueryPort {
  // criteria key는 "attributes.<key>" 또는 "city"/"status" 같은 최상위 필드명
  fun search(tenantId: String, criteria: Map<String, Any?>, limit: Int = 50): List<Member>
}
