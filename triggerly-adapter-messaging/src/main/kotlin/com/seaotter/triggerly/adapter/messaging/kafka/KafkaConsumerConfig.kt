package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer

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
  ): ConcurrentKafkaListenerContainerFactory<String, RawEventMessage> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, RawEventMessage>()
    // Kotlin이 setConsumerFactory/setBatchListener를 프로퍼티(`factory.consumerFactory = ...`,
    // `factory.isBatchListener = ...`)로 인식하지 못해(제네릭 와일드카드 `? super K, ? super V` 시그니처
    // 탓에 getter/setter 쌍이 매칭되지 않아 val로만 노출됨, 실측 확인: "'val' cannot be reassigned" 컴파일
    // 에러) 세터 메서드를 직접 호출한다.
    factory.setConsumerFactory(consumerFactory)
    factory.setBatchListener(true)
    return factory
  }
}
