package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.application.port.MemberQueryPort
import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.Member
import com.seaotter.triggerly.domain.MemberStatus
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.stereotype.Component

@Component
class MemberSearchAdapter(private val operations: ElasticsearchOperations) : MemberQueryPort {

  override fun search(tenantId: String, criteria: Map<String, Any?>, limit: Int): List<Member> {
    var condition = Criteria("tenantId").`is`(tenantId)
    criteria.forEach { (field, value) ->
      if (value != null) condition = condition.and(Criteria(field).`is`(value))
    }
    val query = CriteriaQuery(condition)
    query.setMaxResults(limit)
    return operations.search(query, MemberDocument::class.java).map { it.content.toDomain() }.toList()
  }

  private fun MemberDocument.toDomain() = Member(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.let { DevicePlatform.valueOf(it) },
    gender = gender?.let { Gender.valueOf(it) }, birthday = birthday,
    status = status?.let { MemberStatus.valueOf(it) }, attributes = attributes,
  )
}
