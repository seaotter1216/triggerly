package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaConsumerConfig(
  private val kafkaProperties: KafkaProperties,
  // KafkaProducerConfig와 동일한 이유(Task 13 실측 확인): Spring Boot 4.1에서
  // KafkaProperties.buildConsumerProperties()가 무인자로 바뀌면서 bootstrap-servers를 더 이상 자동
  // 병합하지 않는다. 우리가 직접 만드는 ConsumerFactory 빈에도 (Testcontainers @ServiceConnection이
  // 등록한) KafkaConnectionDetails의 bootstrap-servers를 직접 덮어써야 한다.
  private val connectionDetails: KafkaConnectionDetails,
) {

  @Bean
  fun rawEventConsumerFactory(
    @Value("\${triggerly.kafka.consumer.max-poll-records:500}") maxPollRecords: Int,
  ): ConsumerFactory<String, RawEventMessage> {
    val props = kafkaProperties.buildConsumerProperties().toMutableMap()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = connectionDetails.bootstrapServers
    props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
    props[ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS] = StringDeserializer::class.java
    props[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = JsonDeserializer::class.java
    props[JsonDeserializer.TRUSTED_PACKAGES] = "com.seaotter.triggerly.application.port"
    props[JsonDeserializer.VALUE_DEFAULT_TYPE] = RawEventMessage::class.java.name
    return DefaultKafkaConsumerFactory(props)
  }

  @Bean
  fun rawEventBatchListenerContainerFactory(
    consumerFactory: ConsumerFactory<String, RawEventMessage>,
    rawEventErrorHandler: DefaultErrorHandler,
  ): ConcurrentKafkaListenerContainerFactory<String, RawEventMessage> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, RawEventMessage>()
    // Kotlin이 setConsumerFactory/setBatchListener를 프로퍼티(`factory.consumerFactory = ...`,
    // `factory.isBatchListener = ...`)로 인식하지 못해(제네릭 와일드카드 `? super K, ? super V` 시그니처
    // 탓에 getter/setter 쌍이 매칭되지 않아 val로만 노출됨, 실측 확인: "'val' cannot be reassigned" 컴파일
    // 에러) 세터 메서드를 직접 호출한다.
    factory.setConsumerFactory(consumerFactory)
    factory.setBatchListener(true)
    factory.setCommonErrorHandler(rawEventErrorHandler)
    return factory
  }

  // WorkflowTriggerConsumer가 BatchListenerFailedException(message, cause, index)을 던지면 이 핸들러가
  // 받는다: index 이전 레코드는 이미 처리된 것으로 보고 offset을 커밋하고, index부터는 backOff 간격으로
  // 재시도한다. backOff를 다 쓰고도 실패하면 rawEventDeadLetterRecoverer가 그 레코드 하나만 DLT로 보내고
  // 넘어간다 - 그래야 poison message 하나 때문에 파티션 전체가 멈추지 않는다.
  @Bean
  fun rawEventErrorHandler(
    rawEventDeadLetterRecoverer: DeadLetterPublishingRecoverer,
    @Value("\${triggerly.kafka.consumer.retry.max-attempts:2}") maxRetryAttempts: Long,
    @Value("\${triggerly.kafka.consumer.retry.backoff-ms:1000}") backoffMs: Long,
  ): DefaultErrorHandler = DefaultErrorHandler(rawEventDeadLetterRecoverer, FixedBackOff(backoffMs, maxRetryAttempts))

  // 실패한 원본 레코드를 별도 파티션 계산 없이 항상 DLT 토픽(단일 파티션, KafkaProducerConfig 참고)의
  // 0번 파티션으로 보낸다. rawEventKafkaTemplate은 KafkaProducerConfig가 등록한 빈을 그대로 재사용한다.
  @Bean
  fun rawEventDeadLetterRecoverer(
    rawEventKafkaTemplate: KafkaTemplate<String, RawEventMessage>,
  ): DeadLetterPublishingRecoverer =
    DeadLetterPublishingRecoverer(rawEventKafkaTemplate) { _, _ -> TopicPartition(RAW_EVENTS_DLT_TOPIC, 0) }
}
