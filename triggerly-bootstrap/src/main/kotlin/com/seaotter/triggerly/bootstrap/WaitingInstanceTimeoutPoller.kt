package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

// 스펙 6절 4단계: WAITING인데 waitingUntil이 지난 인스턴스를 찾아 타임아웃 합성 이벤트를
// 같은 Kafka 토픽에 다시 produce한다 — 엔진이 실제 노드 전이는 컨슈머 경로 하나로만 처리하게 하기 위함.
@Component
class WaitingInstanceTimeoutPoller(
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val eventPublisherPort: EventPublisherPort,
) {
  private val log = LoggerFactory.getLogger(WaitingInstanceTimeoutPoller::class.java)

  @Scheduled(fixedDelayString = "\${triggerly.scheduler.timeout-poll-interval-ms:10000}")
  fun pollExpiredInstances() {
    val expired = workflowInstanceRepositoryPort.findWaitingExpired(LocalDateTime.now())
    expired.forEach { instance ->
      log.info("타임아웃 감지: instanceId=${instance.id} node=${instance.currentNodeId}")
      eventPublisherPort.publish(
        RawEventMessage(
          tenantId = instance.tenantId,
          eventCode = "__TIMEOUT__",
          externalMemberId = null,
          memberContext = null,
          attributes = null,
          occurredAt = LocalDateTime.now(),
          syntheticTimeoutForInstanceId = instance.id,
        ),
      )
    }
  }
}
