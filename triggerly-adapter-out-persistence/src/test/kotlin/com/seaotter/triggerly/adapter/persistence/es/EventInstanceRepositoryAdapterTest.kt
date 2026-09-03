package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.ElasticsearchIntegrationTest
import com.seaotter.triggerly.domain.EventInstance
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class EventInstanceRepositoryAdapterTest : ElasticsearchIntegrationTest() {

  @Autowired lateinit var adapter: EventInstanceRepositoryAdapter
  @Autowired lateinit var statsPort: MemberEventStatsAdapter
  @Autowired lateinit var operations: ElasticsearchOperations

  @BeforeEach
  fun setUp() {
    val indexOps = operations.indexOps(EventLogDocument::class.java)
    if (!indexOps.exists()) indexOps.createWithMapping()
  }

  @Test
  fun `save는 MySQL과 ES에 모두 저장되고, MemberEventStatsPort는 최근 N일 카운트를 센다`() {
    val tenantId = "t-${UUID.randomUUID()}"
    val memberId = "m-${UUID.randomUUID()}"
    repeat(3) {
      adapter.save(
        EventInstance(
          id = UUID.randomUUID().toString(), tenantId = tenantId, eventCode = "REVIEW_ADD",
          occurredAt = LocalDateTime.now(), memberId = memberId,
        ),
      )
    }
    adapter.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = tenantId, eventCode = "REVIEW_ADD",
        occurredAt = LocalDateTime.now().minusDays(40), memberId = memberId,
      ),
    )
    operations.indexOps(EventLogDocument::class.java).refresh()

    val count = statsPort.countEvents(tenantId, memberId, "REVIEW_ADD", 30)

    assertEquals(3, count)
  }

  @Test
  fun `findExistingIds는 saveAll로 저장한 id만 골라 반환한다`() {
    val instances = listOf(
      EventInstance(id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "LOGIN", occurredAt = LocalDateTime.now()),
      EventInstance(id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "LOGIN", occurredAt = LocalDateTime.now()),
    )
    adapter.saveAll(instances)
    val notSavedId = UUID.randomUUID().toString()

    val existingIds = adapter.findExistingIds(instances.map { it.id } + notSavedId)

    assertEquals(instances.map { it.id }.toSet(), existingIds)
  }
}
