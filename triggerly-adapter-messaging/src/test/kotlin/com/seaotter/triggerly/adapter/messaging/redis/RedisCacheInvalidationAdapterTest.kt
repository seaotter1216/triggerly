package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.adapter.messaging.RedisIntegrationTest
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
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

class RedisCacheInvalidationAdapterTest : RedisIntegrationTest() {

  @Autowired lateinit var adapter: RedisCacheInvalidationAdapter
  @Autowired lateinit var connectionFactory: RedisConnectionFactory

  @Test
  fun `publish하면 해당 채널을 구독 중인 리스너가 메시지를 받는다`() {
    val latch = CountDownLatch(1)
    var received: String? = null
    val container = RedisMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.addMessageListener(
      MessageListener { message, _ -> received = String(message.body); latch.countDown() },
      ChannelTopic(EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL),
    )
    container.afterPropertiesSet()
    container.start()

    try {
      Thread.sleep(200) // Redis pub/sub는 구독이 실제로 붙은 이후의 메시지만 받으므로 짧게 대기
      adapter.publish(CacheInvalidationTopic.EVENT_DEFINITION, "t1:PURCHASE")
      assertTrue(latch.await(3, TimeUnit.SECONDS))
      assertEquals("t1:PURCHASE", received)
    } finally {
      container.stop()
    }
  }
}
