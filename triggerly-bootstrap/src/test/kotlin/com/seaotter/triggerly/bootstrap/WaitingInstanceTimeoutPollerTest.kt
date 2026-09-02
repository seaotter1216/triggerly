package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class WaitingInstanceTimeoutPollerTest {

  @Test
  fun `만료된 WAITING 인스턴스마다 타임아웃 합성 이벤트를 publish한다`() {
    val instancePort = mockk<WorkflowInstanceRepositoryPort>()
    val publisherPort = mockk<EventPublisherPort>(relaxed = true)
    val expired = WorkflowInstance(
      id = "i1", workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
      version = 1, status = WorkflowInstanceStatus.WAITING,
    )
    every { instancePort.findWaitingExpired(any()) } returns listOf(expired)

    WaitingInstanceTimeoutPoller(instancePort, publisherPort).pollExpiredInstances()

    verify { publisherPort.publish(match { it.syntheticTimeoutForInstanceId == "i1" && it.tenantId == "t1" }) }
  }
}
