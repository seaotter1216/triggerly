package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.adapter.messaging.KafkaIntegrationTest
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import java.time.LocalDateTime
import java.time.Duration

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
  fun `raw-events 토픽에 produce된 메시지는 IngestEventUseCase handle로 전달된다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
    )
    adapter.publish(message)

    Thread.sleep(Duration.ofSeconds(5).toMillis())

    verify(timeout = 5000) { ingestEventUseCase.handle(match { it.eventCode == "LOGIN" && it.externalMemberId == "ext-1" }) }
  }
}
