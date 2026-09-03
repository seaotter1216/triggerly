package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import org.springframework.stereotype.Service

// ActionDispatchConsumer(adapter-messaging)의 진입점. WorkflowEngine의 raw-events 처리 경로와 완전히
// 분리된 컨슈머 그룹에서 호출되므로, 여기서 프로바이더 API가 느려지거나 실패해도 이벤트 수집 쪽 스레드는
// 영향받지 않는다. fire-and-forget이라 결과를 어디에도 다시 쓰지 않는다 - 실패하면 예외를 그대로 던져
// ActionDispatchConsumer 쪽 재시도/DLT가 처리하게 둔다.
@Service
class DispatchActionUseCase(
  private val actionExecutor: ActionExecutor,
) {
  fun handle(message: ActionDispatchMessage) {
    actionExecutor.execute(message.action)
  }
}
