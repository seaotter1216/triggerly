package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.Member
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MemberPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: MemberPersistenceAdapter

  @Test
  fun `Member를 저장하고 externalMemberId로 다시 조회하면 attributes까지 복원된다`() {
    val member = Member(
      id = UUID.randomUUID().toString(),
      tenantId = "t1",
      externalMemberId = "ext-1",
      email = "a@b.com",
      attributes = mapOf("grade" to "VIP"),
    )
    adapter.save(member)

    val found = adapter.findByExternalId("t1", "ext-1")

    assertEquals("a@b.com", found?.email)
    assertEquals("VIP", found?.attributes?.get("grade"))
    assertNull(adapter.findByExternalId("t1", "unknown"))
  }
}
