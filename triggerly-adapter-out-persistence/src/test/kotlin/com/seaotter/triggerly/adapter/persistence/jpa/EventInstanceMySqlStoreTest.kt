package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.EventInstance
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class EventInstanceMySqlStoreTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var store: EventInstanceMySqlStore

  @Test
  fun `저장한 EventInstance를 attributes까지 그대로 복원한다`() {
    val saved = store.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "PURCHASE",
        occurredAt = LocalDateTime.now(), memberId = "m1", attributes = mapOf("amount" to 50000),
      ),
    )
    assertEquals(50000, saved.attributes?.get("amount"))
  }

  @Test
  fun `deleteOlderThan은 cutoff보다 오래된 행만 지운다`() {
    val old = store.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "OLD",
        occurredAt = LocalDateTime.now().minusDays(31),
      ),
    )
    val recent = store.save(
      EventInstance(
        id = UUID.randomUUID().toString(), tenantId = "t1", eventCode = "RECENT",
        occurredAt = LocalDateTime.now(),
      ),
    )

    val deleted = store.deleteOlderThan(LocalDateTime.now().minusDays(30))

    assertEquals(1, deleted)
  }
}
