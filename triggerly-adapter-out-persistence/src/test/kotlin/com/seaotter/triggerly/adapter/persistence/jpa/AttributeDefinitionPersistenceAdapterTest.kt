package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals

class AttributeDefinitionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: AttributeDefinitionPersistenceAdapter

  @Test
  fun `저장한 AttributeDefinition을 테넌트별로 조회할 수 있다`() {
    adapter.save(
      AttributeDefinition(
        id = UUID.randomUUID().toString(), tenantId = "t1", key = "amount",
        displayName = "결제 금액", type = AttributeType.LONG, filterable = true,
      ),
    )
    val list = adapter.findAll("t1")
    assertEquals(1, list.size)
    assertEquals(AttributeType.LONG, list[0].type)
  }
}
