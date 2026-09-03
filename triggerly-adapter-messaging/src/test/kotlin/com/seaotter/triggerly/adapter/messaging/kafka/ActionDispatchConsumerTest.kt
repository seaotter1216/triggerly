package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.usecase.DispatchActionUseCase
import com.seaotter.triggerly.domain.ActionDefinition
import io.mockk.every
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import java.time.Duration
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// @Import 필요 이유는 WorkflowTriggerConsumerTest와 동일: KafkaIntegrationTest가 명시적으로 지정한
// @SpringBootTest(classes=[...]) 아래에서는 중첩 @TestConfiguration이 자동 감지되지 않는다.
@Import(ActionDispatchConsumerTest.MockConfig::class)
class ActionDispatchConsumerTest : KafkaIntegrationTest() {

  @Autowired lateinit var adapter: ActionDispatchPublisherAdapter

  @org.springframework.boot.test.context.TestConfiguration
  class MockConfig {
    // MessagingTestApplication의 defaultDispatchActionUseCase() 안전망 빈과 타입이 같으므로 @Primary로
    // 명시해 이 테스트의 mock이 ActionDispatchConsumer에 주입되도록 한다.
    @Primary
    @org.springframework.context.annotation.Bean
    fun dispatchActionUseCase(): DispatchActionUseCase = io.mockk.mockk(relaxed = true)
  }

  @Autowired lateinit var dispatchActionUseCase: DispatchActionUseCase

  @Test
  fun `actions dispatch 토픽에 produce된 메시지는 DispatchActionUseCase handle로 전달된다`() {
    val dispatchId = "dispatch-happy-${System.nanoTime()}"
    val message = ActionDispatchMessage(
      tenantId = "t1", memberId = "m1", action = ActionDefinition.IssueCoupon("BIRTHDAY10"),
      workflowInstanceId = "wf-instance-1", nodeId = "n3", dispatchId = dispatchId,
    )
    adapter.publish(message)

    verify(timeout = 5000) {
      dispatchActionUseCase.handle(match { it.dispatchId == dispatchId })
    }
  }

  // 이 dispatchId를 담은 메시지만 실패하도록 좁혀서 스텁한다 - 다른 테스트가 발행한 메시지는 그대로
  // MockConfig의 기본 relaxed mock(무동작) 경로를 타야 하므로, handle 전체를 무조건 실패시키면 안 된다.
  @Test
  fun `handle이 반복 실패하면 재시도를 다 쓴 뒤 DLT 토픽으로 전송된다`() {
    val dispatchId = "dispatch-dlt-${System.nanoTime()}"
    every {
      dispatchActionUseCase.handle(match { it.dispatchId == dispatchId })
    } throws RuntimeException("의도적 실패 (DLT 라우팅 테스트)")

    val message = ActionDispatchMessage(
      tenantId = "t1", memberId = "m1", action = ActionDefinition.IssueCoupon("BIRTHDAY10"),
      workflowInstanceId = "wf-instance-2", nodeId = "n3", dispatchId = dispatchId,
    )
    adapter.publish(message)

    val props = Properties().apply {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIntegrationTest.kafka.bootstrapServers)
      put(ConsumerConfig.GROUP_ID_CONFIG, "action-dlt-test-consumer-${System.nanoTime()}")
      put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
    }
    KafkaConsumer<String, String>(props).use { consumer ->
      consumer.subscribe(listOf(ACTION_DISPATCH_DLT_TOPIC))
      val records = consumer.poll(Duration.ofSeconds(20))
      assertEquals(1, records.count())
      assertTrue(records.first().value().contains(dispatchId))
    }
  }
}
