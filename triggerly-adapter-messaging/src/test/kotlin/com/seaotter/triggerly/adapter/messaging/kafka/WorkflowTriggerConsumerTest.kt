package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.usecase.BatchEventProcessingException
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import io.mockk.every
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import java.time.LocalDateTime
import java.time.Duration
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// @SpringBootTest(classes = [...])가 KafkaIntegrationTest에서 명시적으로 설정되어 있으면 Spring Boot가
// 중첩 @TestConfiguration을 자동으로 감지하지 않는다(실측 확인: @Import 없이는
// NoSuchBeanDefinitionException으로 IngestEventUseCase 빈을 못 찾음). 브리프의 테스트 코드에는 없지만
// 최소한의 수정으로 @Import를 추가한다.
@Import(WorkflowTriggerConsumerTest.MockConfig::class)
class WorkflowTriggerConsumerTest : KafkaIntegrationTest() {

  @Autowired lateinit var adapter: KafkaEventPublisherAdapter

  @org.springframework.boot.test.context.TestConfiguration
  class MockConfig {
    // MessagingTestApplication의 defaultIngestEventUseCase() 안전망 빈과 타입이 같으므로 @Primary로
    // 명시해 이 테스트의 mock이 WorkflowTriggerConsumer에 주입되도록 한다(MessagingTestApplication.kt 참고).
    @Primary
    @org.springframework.context.annotation.Bean
    fun ingestEventUseCase(): IngestEventUseCase = io.mockk.mockk(relaxed = true)
  }

  @Autowired lateinit var ingestEventUseCase: IngestEventUseCase

  @Test
  fun `raw-events 토픽에 produce된 메시지는 IngestEventUseCase handleBatch로 전달된다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
    )
    adapter.publish(message)

    Thread.sleep(Duration.ofSeconds(5).toMillis())

    verify(timeout = 5000) {
      ingestEventUseCase.handleBatch(match { it.any { m -> m.eventCode == "LOGIN" && m.externalMemberId == "ext-1" } })
    }
  }

  // 이 eventId를 담은 배치만 실패하도록 좁혀서 스텁한다 - 다른 테스트가 발행한 메시지는 그대로
  // MockConfig의 기본 relaxed mock(무동작) 경로를 타야 하므로, handleBatch 전체를 무조건 실패시키면 안 된다.
  @Test
  fun `handleBatch가 반복 실패하면 재시도를 다 쓴 뒤 DLT 토픽으로 전송된다`() {
    val dltEventId = "evt-dlt-${System.nanoTime()}"
    every {
      ingestEventUseCase.handleBatch(match { messages -> messages.any { it.eventId == dltEventId } })
    } throws BatchEventProcessingException(0, dltEventId, RuntimeException("의도적 실패 (DLT 라우팅 테스트)"))

    val message = RawEventMessage(
      tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-dlt",
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(), eventId = dltEventId,
    )
    adapter.publish(message)

    // KafkaEventPublisherAdapterTest와 같은 방식: 별도의 원시 KafkaConsumer로 DLT 토픽을 직접 구독해
    // rawEventErrorHandler(FixedBackOff 2회 재시도) + rawEventDeadLetterRecoverer가 실제로 동작해
    // 이 레코드를 raw-events.DLT로 옮기는지 확인한다.
    val props = Properties().apply {
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIntegrationTest.kafka.bootstrapServers)
      put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-consumer-${System.nanoTime()}")
      put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
    }
    KafkaConsumer<String, String>(props).use { consumer ->
      consumer.subscribe(listOf(RAW_EVENTS_DLT_TOPIC))
      val records = consumer.poll(Duration.ofSeconds(20))
      assertEquals(1, records.count())
      assertTrue(records.first().value().contains(dltEventId))
    }
  }
}
