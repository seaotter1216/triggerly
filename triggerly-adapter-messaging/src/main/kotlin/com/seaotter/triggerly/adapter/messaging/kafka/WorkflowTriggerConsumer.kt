package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class WorkflowTriggerConsumer(private val ingestEventUseCase: IngestEventUseCase) {

  private val log = LoggerFactory.getLogger(WorkflowTriggerConsumer::class.java)

  @KafkaListener(
    topics = [RAW_EVENTS_TOPIC],
    groupId = "triggerly-engine",
    concurrency = "\${triggerly.kafka.consumer.concurrency:8}",
    containerFactory = "rawEventBatchListenerContainerFactory",
  )
  fun onMessages(records: List<ConsumerRecord<String, RawEventMessage>>) {
    records.forEach { record ->
      runCatching { ingestEventUseCase.handle(record.value()) }
        .onFailure { log.error("이벤트 처리 실패: key=${record.key()}", it) }
    }
  }
}
