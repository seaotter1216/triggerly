package com.seaotter.triggerly.adapter.messaging.kafka

import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.ActionDispatchPort
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class ActionDispatchPublisherAdapter(
  private val kafkaTemplate: KafkaTemplate<String, ActionDispatchMessage>,
) : ActionDispatchPort {

  private val log = LoggerFactory.getLogger(ActionDispatchPublisherAdapter::class.java)

  // KafkaEventPublisherAdapter와 동일하게 fire-and-forget이다 - WorkflowEngine 호출 스레드(raw-events
  // 컨슈머)는 send 완료를 기다리지 않는다. workflowInstanceId를 파티션 키로 써서 같은 인스턴스의 여러
  // Action이 같은 파티션에서 순서대로 처리되게 한다.
  override fun publish(message: ActionDispatchMessage) {
    kafkaTemplate.send(ACTION_DISPATCH_TOPIC, message.workflowInstanceId, message).whenComplete { _, ex ->
      if (ex != null) log.error("액션 디스패치 publish 실패: dispatchId=${message.dispatchId}", ex)
    }
  }
}
