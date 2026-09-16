package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import com.seaotter.triggerly.application.port.LockResult
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals

class RedisDistributedLockAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisDistributedLockAdapter

  @Test
  fun `같은 키로 두 번 tryLock하면 두 번째는 AlreadyHeld를 반환한다`() {
    val key = "lock:test:${System.nanoTime()}"
    assertEquals(LockResult.Acquired, adapter.tryLock(key, Duration.ofSeconds(10)))
    assertEquals(LockResult.AlreadyHeld, adapter.tryLock(key, Duration.ofSeconds(10)))
  }

  @Test
  fun `ttl이 지나면 같은 키를 다시 tryLock할 수 있다`() {
    val key = "lock:ttl:${System.nanoTime()}"
    assertEquals(LockResult.Acquired, adapter.tryLock(key, Duration.ofMillis(200)))
    Thread.sleep(300)
    assertEquals(LockResult.Acquired, adapter.tryLock(key, Duration.ofMillis(200)))
  }

  @Test
  fun `release 이후에는 같은 키로 다시 tryLock할 수 있다`() {
    val key = "lock:release:${System.nanoTime()}"
    assertEquals(LockResult.Acquired, adapter.tryLock(key, Duration.ofSeconds(10)))
    adapter.release(key)
    assertEquals(LockResult.Acquired, adapter.tryLock(key, Duration.ofSeconds(10)))
  }
}
