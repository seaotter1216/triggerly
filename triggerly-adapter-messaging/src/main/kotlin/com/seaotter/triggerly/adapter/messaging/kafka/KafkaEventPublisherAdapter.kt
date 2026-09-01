package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RawEventMessage
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaEventPublisherAdapter(
  private val kafkaTemplate: KafkaTemplate<String, RawEventMessage>,
) : EventPublisherPort {

  private val log = LoggerFactory.getLogger(KafkaEventPublisherAdapter::class.java)

  // 13.1절: 컨트롤러 스레드는 produce 완료를 기다리지 않는다 — 실패는 로깅만 하고 요청 흐름을 막지 않는다.
  override fun publish(message: RawEventMessage) {
    val key = "${message.tenantId}:${message.externalMemberId ?: "anon"}"
    kafkaTemplate.send(RAW_EVENTS_TOPIC, key, message).whenComplete { _, ex ->
      if (ex != null) log.error("Kafka publish 실패: key=$key eventCode=${message.eventCode}", ex)
    }
  }
}
