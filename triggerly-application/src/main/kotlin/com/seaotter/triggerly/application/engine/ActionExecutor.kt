package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.domain.ActionDefinition
import com.seaotter.triggerly.domain.describe
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

// ActionDispatchConsumer(별도 컨슈머 그룹, adapter-messaging)가 호출한다 - WorkflowEngine이 아니다.
// 실제 쿠폰/알림톡/푸시 프로바이더 연동은 아직 데모 로그로 남겨두고, 이 자리에 나중에 HTTP 호출을 붙인다.
@Component
class ActionExecutor {
  private val log = LoggerFactory.getLogger(ActionExecutor::class.java)

  fun execute(action: ActionDefinition): String {
    val description = action.describe()
    log.info("[DEMO ACTION] {}", description)
    return description
  }
}
