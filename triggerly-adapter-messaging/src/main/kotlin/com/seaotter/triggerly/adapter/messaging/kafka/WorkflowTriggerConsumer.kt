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
  // handleBatch 하나로 배치 전체의 멤버/EventInstance DB 왕복을 묶는다. 대신 이 배치 안의 레코드 하나가
  // 실패하면(예: DB 오류) 배치 전체가 실패로 잡히고, 이미 처리된 레코드까지 포함해 offset은 그대로
  // 커밋된다 - 레코드 단위 격리를 배치 처리 효율과 맞바꾼 트레이드오프다.
  fun onMessages(records: List<ConsumerRecord<String, RawEventMessage>>) {
    runCatching { ingestEventUseCase.handleBatch(records.map { it.value() }) }
      .onFailure { log.error("배치 이벤트 처리 실패: size=${records.size}", it) }
  }
}
