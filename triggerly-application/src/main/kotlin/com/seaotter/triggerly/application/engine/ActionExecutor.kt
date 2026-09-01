package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.domain.ActionDefinition
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ActionExecutor {
  private val log = LoggerFactory.getLogger(ActionExecutor::class.java)

  fun execute(action: ActionDefinition): String {
    val description = when (action) {
      is ActionDefinition.IssueCoupon -> "쿠폰 발급: couponId=${action.couponId}"
      is ActionDefinition.SendPush -> "푸시 발송: templateId=${action.templateId}"
      is ActionDefinition.SendAlimTalk -> "알림톡 발송: templateId=${action.templateId}"
    }
    log.info("[DEMO ACTION] {}", description)
    return description
  }
}
