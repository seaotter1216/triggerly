package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.EventDefinition
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertEquals
import kotlin.test.assertNull

class EventDefinitionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: EventDefinitionPersistenceAdapter

  @Test
  fun `저장한 EventDefinition을 tenantId와 code로 조회할 수 있다`() {
    adapter.save(EventDefinition(tenantId = "t1", code = "PURCHASE", displayName = "구매"))

    val found = adapter.findByTenantAndCode("t1", "PURCHASE")

    assertEquals("구매", found?.displayName)
    assertNull(adapter.findByTenantAndCode("t1", "UNKNOWN"))
  }

  @Test
  fun `findAll은 테넌트별로만 조회한다`() {
    adapter.save(EventDefinition(tenantId = "t1", code = "A", displayName = "A"))
    adapter.save(EventDefinition(tenantId = "t2", code = "A", displayName = "A"))

    assertEquals(1, adapter.findAll("t1").size)
  }
}
