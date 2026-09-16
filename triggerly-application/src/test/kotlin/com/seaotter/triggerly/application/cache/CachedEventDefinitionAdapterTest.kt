package com.seaotter.triggerly.application.cache

import com.github.benmanes.caffeine.cache.Ticker
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CachedEventDefinitionAdapterTest {

  private val delegate = mockk<EventDefinitionPort>()

  @Test
  fun `동일 tenantId+code를 반복 조회해도 delegate는 한 번만 호출된다`() {
    val definition = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.findByTenantAndCode("t1", "PURCHASE") } returns definition
    val cache = CachedEventDefinitionAdapter(delegate)

    repeat(3) { assertEquals(definition, cache.findByTenantAndCode("t1", "PURCHASE")) }

    verify(exactly = 1) { delegate.findByTenantAndCode("t1", "PURCHASE") }
  }

  @Test
  fun `evict 이후에는 delegate를 다시 호출한다`() {
    val definition = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.findByTenantAndCode("t1", "PURCHASE") } returns definition
    val cache = CachedEventDefinitionAdapter(delegate)

    cache.findByTenantAndCode("t1", "PURCHASE")
    cache.evict("t1", "PURCHASE")
    cache.findByTenantAndCode("t1", "PURCHASE")

    verify(exactly = 2) { delegate.findByTenantAndCode("t1", "PURCHASE") }
  }

  @Test
  fun `TTL이 지나면 delegate를 다시 호출한다`() {
    val definition = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.findByTenantAndCode("t1", "PURCHASE") } returns definition
    val nanos = AtomicLong(0)
    val ticker = Ticker { nanos.get() }
    val cache = CachedEventDefinitionAdapter(delegate, ttl = Duration.ofSeconds(60), ticker = ticker)

    cache.findByTenantAndCode("t1", "PURCHASE")
    nanos.set(Duration.ofSeconds(61).toNanos())
    cache.findByTenantAndCode("t1", "PURCHASE")

    verify(exactly = 2) { delegate.findByTenantAndCode("t1", "PURCHASE") }
  }

  @Test
  fun `존재하지 않는 정의도 null로 캐시되어 delegate를 반복 호출하지 않는다`() {
    every { delegate.findByTenantAndCode("t1", "UNKNOWN") } returns null
    val cache = CachedEventDefinitionAdapter(delegate)

    assertNull(cache.findByTenantAndCode("t1", "UNKNOWN"))
    assertNull(cache.findByTenantAndCode("t1", "UNKNOWN"))

    verify(exactly = 1) { delegate.findByTenantAndCode("t1", "UNKNOWN") }
  }

  @Test
  fun `findAll과 save는 캐시를 거치지 않고 delegate에 그대로 위임한다`() {
    every { delegate.findAll("t1") } returns emptyList()
    val saved = EventDefinition("t1", "PURCHASE", "구매")
    every { delegate.save(any()) } returns saved
    val cache = CachedEventDefinitionAdapter(delegate)

    cache.findAll("t1")
    cache.save(saved)

    verify(exactly = 1) { delegate.findAll("t1") }
    verify(exactly = 1) { delegate.save(saved) }
  }
}
