package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.application.port.MemberEventStatsPort
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class MemberEventStatsAdapter(private val operations: ElasticsearchOperations) : MemberEventStatsPort {

  override fun countEvents(tenantId: String, memberId: String, eventCode: String, withinDays: Int): Long {
    val since = LocalDateTime.now().minusDays(withinDays.toLong())
    val condition = Criteria("tenantId").`is`(tenantId)
      .and(Criteria("memberId").`is`(memberId))
      .and(Criteria("eventCode").`is`(eventCode))
      .and(Criteria("occurredAt").greaterThanEqual(since))
    return operations.count(CriteriaQuery(condition), EventLogDocument::class.java)
  }
}
