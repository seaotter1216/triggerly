package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.EventDefinition
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull

// MySqlIntegrationTest는 테스트 클래스 사이에 컨테이너/데이터를 초기화하지 않는 싱글턴 컨테이너
// 패턴을 쓰므로(Task 7 참고), 테스트 간 데이터 충돌을 피하려면 테넌트ID를 테스트마다 고유하게 둔다.
class EventDefinitionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: EventDefinitionPersistenceAdapter

  @Test
  fun `저장한 EventDefinition을 tenantId와 code로 조회할 수 있다`() {
    val tenantId = "t-${UUID.randomUUID()}"
    adapter.save(EventDefinition(tenantId = tenantId, code = "PURCHASE", displayName = "구매"))

    val found = adapter.findByTenantAndCode(tenantId, "PURCHASE")

    assertEquals("구매", found?.displayName)
    assertNull(adapter.findByTenantAndCode(tenantId, "UNKNOWN"))
  }

  @Test
  fun `findAll은 테넌트별로만 조회한다`() {
    val tenantId1 = "t-${UUID.randomUUID()}"
    val tenantId2 = "t-${UUID.randomUUID()}"
    adapter.save(EventDefinition(tenantId = tenantId1, code = "A", displayName = "A"))
    adapter.save(EventDefinition(tenantId = tenantId2, code = "A", displayName = "A"))

    assertEquals(1, adapter.findAll(tenantId1).size)
  }
}
