package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.usecase.BatchEventProcessingException
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.BatchListenerFailedException
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
    try {
      ingestEventUseCase.handleBatch(records.map { it.value() })
    } catch (ex: BatchEventProcessingException) {
      // handleBatch가 몇 번째 레코드에서 실패했는지 알려주면, 그 인덱스를 스프링 카프카 전용 예외로
      // 감싸서 컨테이너에 전달한다. rawEventBatchListenerContainerFactory에 달린
      // DefaultErrorHandler(+ FixedBackOff + DeadLetterPublishingRecoverer, KafkaConsumerConfig 참고)가
      // 이 예외를 받아: 실패 인덱스 이전 레코드들의 offset은 커밋(재처리 안 됨), 실패한 레코드부터는
      // 설정된 횟수만큼 재시도하다가 그래도 실패하면 DLT(raw-events.DLT) 토픽으로 보내고 다음으로 넘어간다.
      log.error("배치 이벤트 처리 실패: index=${ex.failedIndex} eventId=${ex.eventId}", ex.cause ?: ex)
      throw BatchListenerFailedException(ex.message ?: "배치 이벤트 처리 실패", ex.cause ?: ex, ex.failedIndex)
    }
    // 그 외 예외(예: 루프 진입 전 멤버 벌크 조회/저장 실패)는 그대로 던져 배치 전체가 재시도되게 둔다 -
    // 이 시점에는 어떤 레코드도 실제로 처리(엔진 실행)되지 않았으므로 전체 재시도가 안전하다.
  }
}
