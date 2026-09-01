package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.time.LocalDateTime
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaEventPublisherAdapterTest : KafkaIntegrationTest() {

  @Autowired lateinit var adapter: KafkaEventPublisherAdapter

  @Test
  fun `publish는 tenantId콜론externalMemberId를 키로 raw-events 토픽에 produce한다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "PURCHASE", externalMemberId = "ext-1",
      memberContext = null, attributes = mapOf("amount" to 1000), occurredAt = LocalDateTime.now(),
    )
    adapter.publish(message)

    val props = Properties().apply {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIntegrationTest.kafka.bootstrapServers)
      put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-${System.nanoTime()}")
      put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
    }
    KafkaConsumer<String, String>(props).use { consumer ->
      consumer.subscribe(listOf(RAW_EVENTS_TOPIC))
      val records = consumer.poll(Duration.ofSeconds(10))
      assertEquals(1, records.count())
      val record = records.first()
      assertEquals("t1:ext-1", record.key())
      assertTrue(record.value().contains("PURCHASE"))
    }
  }
}
