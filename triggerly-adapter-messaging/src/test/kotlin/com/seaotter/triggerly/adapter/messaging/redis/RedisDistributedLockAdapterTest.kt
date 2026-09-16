package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RedisDistributedLockAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisDistributedLockAdapter

  @Test
  fun `같은 키로 두 번 tryLock하면 두 번째는 실패한다`() {
    val key = "lock:test:${System.nanoTime()}"
    assertTrue(adapter.tryLock(key, Duration.ofSeconds(10)))
    assertFalse(adapter.tryLock(key, Duration.ofSeconds(10)))
  }

  @Test
  fun `ttl이 지나면 같은 키를 다시 tryLock할 수 있다`() {
    val key = "lock:ttl:${System.nanoTime()}"
    assertTrue(adapter.tryLock(key, Duration.ofMillis(200)))
    Thread.sleep(300)
    assertTrue(adapter.tryLock(key, Duration.ofMillis(200)))
  }

  @Test
  fun `release 이후에는 같은 키로 다시 tryLock할 수 있다`() {
    val key = "lock:release:${System.nanoTime()}"
    assertTrue(adapter.tryLock(key, Duration.ofSeconds(10)))
    adapter.release(key)
    assertTrue(adapter.tryLock(key, Duration.ofSeconds(10)))
  }
}
