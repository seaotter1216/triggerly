package com.seaotter.triggerly.bootstrap.cache

import com.seaotter.triggerly.adapter.messaging.redis.EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL
import com.seaotter.triggerly.adapter.persistence.jpa.EventDefinitionPersistenceAdapter
import com.seaotter.triggerly.application.cache.CachedEventDefinitionAdapter
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.bootstrap.FullStackIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// "다른 앱 인스턴스"를 흉내내기 위해, 스프링이 관리하는 캐시(이 테스트 프로세스 자체)와는 별개로 같은
// EventDefinitionPersistenceAdapter delegate를 감싼 두 번째 CachedEventDefinitionAdapter(instance B)를
// 수동으로 만들고, 같은 Redis 채널을 구독하는 별도의 RedisMessageListenerContainer를 붙인다.
// ManageEventDefinitionUseCase(instance A 경유)에서 저장이 일어나면 instance B도 Redis pub/sub만으로
// 즉시 evict되는지 검증한다 - 두 인스턴스는 서로를 직접 참조하지 않는다.
class ReferenceDataCacheCrossInstanceTest : FullStackIntegrationTest() {

  @Autowired lateinit var manageEventDefinitionUseCase: ManageEventDefinitionUseCase
  @Autowired lateinit var delegate: EventDefinitionPersistenceAdapter
  @Autowired lateinit var connectionFactory: RedisConnectionFactory

  @Test
  fun `한 인스턴스에서 저장하면 다른 인스턴스의 캐시도 Redis pub-sub만으로 무효화된다`() {
    val tenantId = "t-cross-${System.nanoTime()}"
    manageEventDefinitionUseCase.register(tenantId, "PURCHASE", "구매")

    val instanceBCache = CachedEventDefinitionAdapter(delegate)
    val warmed = instanceBCache.findByTenantAndCode(tenantId, "PURCHASE")
    assertEquals("구매", warmed?.displayName)

    val latch = CountDownLatch(1)
    val container = RedisMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.addMessageListener(
      MessageListener { message, _ ->
        val (t, code) = String(message.body).split(":", limit = 2)
        instanceBCache.evict(t, code)
        latch.countDown()
      },
      ChannelTopic(EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL),
    )
    container.afterPropertiesSet()
    container.start()

    try {
      Thread.sleep(200)
      manageEventDefinitionUseCase.register(tenantId, "PURCHASE", "구매(수정)")

      assertTrue(latch.await(3, TimeUnit.SECONDS), "무효화 메시지를 3초 안에 받지 못했다")
      assertEquals("구매(수정)", instanceBCache.findByTenantAndCode(tenantId, "PURCHASE")?.displayName)
    } finally {
      container.stop()
    }
  }
}
