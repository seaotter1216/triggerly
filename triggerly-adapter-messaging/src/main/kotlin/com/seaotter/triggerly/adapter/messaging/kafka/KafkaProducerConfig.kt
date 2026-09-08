@file:Suppress("DEPRECATION") // JsonSerializer 사용 이유는 아래 RAW_EVENTS_TOPIC 상단 주석 참고

package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

// JsonSerializer(Jackson2 기반)는 spring-kafka 4.0부터 @Deprecated(forRemoval=true) 상태다.
// 대체품인 JacksonJsonSerializer(Jackson3 기반, tools.jackson.databind.json.JsonMapper 사용)로
// 바꾸고 싶지만, 이 프로젝트의 RawEventMessage/ActionDispatchMessage가 Kotlin data class이고
// Jackson3용 jackson-module-kotlin이 아직 Maven Central에 없어(실측 확인, 2026-09-08) 전환 시
// application.yml에 남겨둔 것과 동일한 "no Creators" 역직렬화 예외가 재현될 가능성이 높다.
// jackson-module-kotlin이 Jackson3를 지원하기 시작하면 그때 JacksonJsonSerializer로 교체할 것.
const val RAW_EVENTS_TOPIC = "triggerly.events.raw"

// 재시도(FixedBackOff)를 다 써도 실패하는 레코드가 최종적으로 도착하는 곳. 파티션 병렬성이 필요 없는
// 보관함일 뿐이라 파티션 1개로 충분하다 (KafkaConsumerConfig의 DeadLetterPublishingRecoverer가 사용).
const val RAW_EVENTS_DLT_TOPIC = "$RAW_EVENTS_TOPIC.DLT"

// Action 노드 실행을 raw-events 수집 컨슈머 스레드에서 떼어내기 위한 fire-and-forget 디스패치 토픽.
// WorkflowEngine이 여기 발행만 하고 바로 다음 노드로 진행하면, ActionDispatchConsumer가 완전히 독립된
// 컨슈머 그룹/스레드 풀에서 실제 프로바이더 호출을 한다 - 프로바이더가 느려져도 raw-events 파티션은
// 영향받지 않는다.
const val ACTION_DISPATCH_TOPIC = "triggerly.actions.dispatch"
const val ACTION_DISPATCH_DLT_TOPIC = "$ACTION_DISPATCH_TOPIC.DLT"

@Configuration
class KafkaProducerConfig(
  private val kafkaProperties: KafkaProperties,
  // Spring Boot 4.1에서 KafkaProperties.buildProducerProperties()가 무인자로 바뀌면서(브리프가 가정한
  // buildProducerProperties(connectionDetails) 시그니처는 더 이상 존재하지 않음) bootstrap-servers를 더 이상
  // 자동 병합하지 않는다. Boot 자체 자동구성(KafkaAutoConfiguration.kafkaProducerFactory)은 내부적으로
  // KafkaConnectionDetails를 병합해 처리하지만, 우리가 직접 만드는 ProducerFactory 빈에는 적용되지 않으므로
  // (Testcontainers @ServiceConnection이 등록한) KafkaConnectionDetails 빈을 주입받아 bootstrap-servers를
  // 직접 덮어써야 한다(실측 확인: 병합 없이는 기본값 localhost:9092로 접속을 시도하다 타임아웃).
  private val connectionDetails: KafkaConnectionDetails,
) {

  @Bean
  fun rawEventsTopic(
    @Value("\${triggerly.kafka.raw-events-topic.partitions:32}") partitions: Int,
  ): NewTopic = TopicBuilder.name(RAW_EVENTS_TOPIC).partitions(partitions).replicas(1).build()

  @Bean
  fun rawEventsDeadLetterTopic(): NewTopic = TopicBuilder.name(RAW_EVENTS_DLT_TOPIC).partitions(1).replicas(1).build()

  @Bean
  fun rawEventProducerFactory(): ProducerFactory<String, RawEventMessage> {
    val props = kafkaProperties.buildProducerProperties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = connectionDetails.bootstrapServers
    return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer())
  }

  @Bean
  fun rawEventKafkaTemplate(producerFactory: ProducerFactory<String, RawEventMessage>): KafkaTemplate<String, RawEventMessage> =
    KafkaTemplate(producerFactory)

  @Bean
  fun actionDispatchTopic(
    @Value("\${triggerly.kafka.action-dispatch-topic.partitions:16}") partitions: Int,
  ): NewTopic = TopicBuilder.name(ACTION_DISPATCH_TOPIC).partitions(partitions).replicas(1).build()

  @Bean
  fun actionDispatchDeadLetterTopic(): NewTopic = TopicBuilder.name(ACTION_DISPATCH_DLT_TOPIC).partitions(1).replicas(1).build()

  @Bean
  fun actionDispatchProducerFactory(): ProducerFactory<String, ActionDispatchMessage> {
    val props = kafkaProperties.buildProducerProperties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = connectionDetails.bootstrapServers
    return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer())
  }

  @Bean
  fun actionDispatchKafkaTemplate(producerFactory: ProducerFactory<String, ActionDispatchMessage>): KafkaTemplate<String, ActionDispatchMessage> =
    KafkaTemplate(producerFactory)
}
