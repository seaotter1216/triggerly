package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.domain.ActionDefinition
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ActionDispatchPublisherAdapterTest : KafkaIntegrationTest() {

  @Autowired lateinit var adapter: ActionDispatchPublisherAdapter

  @Test
  fun `publish는 workflowInstanceId를 키로 actions dispatch 토픽에 produce한다`() {
    val dispatchId = "dispatch-${System.nanoTime()}"
    val message = ActionDispatchMessage(
      tenantId = "t1", memberId = "m1", action = ActionDefinition.IssueCoupon("BIRTHDAY10"),
      workflowInstanceId = "wf-instance-1", nodeId = "n3", dispatchId = dispatchId,
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
      consumer.subscribe(listOf(ACTION_DISPATCH_TOPIC))
      // 이 토픽은 ActionDispatchConsumerTest도 공유(싱글턴 컨테이너 패턴)하므로, earliest 오프셋부터 읽으면
      // 다른 테스트가 발행한 레코드도 섞여 들어온다 - 전체 개수 대신 이 dispatchId로 찾은 레코드 1건만 검증한다.
      val record = pollUntilFound(consumer, dispatchId)
      assertEquals("wf-instance-1", record.key())
      assertTrue(record.value().contains("BIRTHDAY10"))
    }
  }

  private fun pollUntilFound(consumer: KafkaConsumer<String, String>, dispatchId: String) =
    generateSequence { consumer.poll(Duration.ofSeconds(2)) }
      .take(5)
      .flatMap { it.iterator().asSequence() }
      .firstOrNull { it.value().contains(dispatchId) }
      ?: error("dispatchId=$dispatchId 레코드를 찾지 못함")
}
