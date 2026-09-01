package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RedisWaitingIndexAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisWaitingIndexAdapter

  @Test
  fun `register한 인스턴스를 lookup으로 찾고, remove하면 사라진다`() {
    adapter.register("t1", "PURCHASE", "m1", "instance-1")
    adapter.register("t1", "PURCHASE", "m1", "instance-2")

    assertEquals(listOf("instance-1", "instance-2"), adapter.lookup("t1", "PURCHASE", "m1"))

    adapter.remove("t1", "PURCHASE", "m1", "instance-1")

    assertEquals(listOf("instance-2"), adapter.lookup("t1", "PURCHASE", "m1"))
    assertTrue(adapter.lookup("t1", "OTHER_EVENT", "m1").isEmpty())
  }
}
