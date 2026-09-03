package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.usecase.DispatchActionUseCase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class ActionDispatchConsumer(private val dispatchActionUseCase: DispatchActionUseCase) {

  // raw-events 처리(WorkflowTriggerConsumer, groupId=triggerly-engine)와 완전히 분리된 컨슈머 그룹/스레드
  // 풀이라, 여기서 프로바이더 API 호출이 느려지거나 실패해도 이벤트 수집 쪽은 영향받지 않는다. 예외를
  // 던지면 actionDispatchListenerContainerFactory에 달린 DefaultErrorHandler(+FixedBackOff+DLT)가
  // 그대로 처리한다 - 레코드 단위 리스너라 raw-events처럼 실패 인덱스를 따로 감쌀 필요가 없다.
  @KafkaListener(
    topics = [ACTION_DISPATCH_TOPIC],
    groupId = "triggerly-action-dispatch",
    concurrency = "\${triggerly.kafka.action-dispatch.consumer.concurrency:16}",
    containerFactory = "actionDispatchListenerContainerFactory",
  )
  fun onMessage(record: ConsumerRecord<String, ActionDispatchMessage>) {
    dispatchActionUseCase.handle(record.value())
  }
}
