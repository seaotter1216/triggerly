package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestPropertySource(properties = ["triggerly.ratelimit.default-rps=3"])
class RedisRateLimiterAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisRateLimiterAdapter

  @Test
  fun `설정된 rps를 초과하면 tryConsume이 false를 반환한다`() {
    val tenantId = "t-ratelimit-${System.nanoTime()}"
    assertTrue(adapter.tryConsume(tenantId))
    assertTrue(adapter.tryConsume(tenantId))
    assertTrue(adapter.tryConsume(tenantId))
    assertFalse(adapter.tryConsume(tenantId))
  }
}
