package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.domain.ActionDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class DispatchActionUseCaseTest {

  private val actionExecutor = mockk<ActionExecutor>(relaxed = true)
  private val distributedLockPort = mockk<DistributedLockPort>()
  private val useCase = DispatchActionUseCase(actionExecutor, distributedLockPort)

  private fun message(dispatchId: String) = ActionDispatchMessage(
    tenantId = "t1", memberId = "m1", action = ActionDefinition.IssueCoupon("BIRTHDAY10"),
    workflowInstanceId = "wf-instance-1", nodeId = "n3", dispatchId = dispatchId,
  )

  @Test
  fun `dedup 락을 처음 선점하면 ActionExecutor를 호출한다`() {
    every { distributedLockPort.tryLock("dispatch-dedup:dispatch-1", any()) } returns true

    useCase.handle(message("dispatch-1"))

    verify(exactly = 1) { actionExecutor.execute(any()) }
  }

  @Test
  fun `dedup 락을 이미 선점한 상태면 ActionExecutor를 호출하지 않는다`() {
    every { distributedLockPort.tryLock("dispatch-dedup:dispatch-1", any()) } returns false

    useCase.handle(message("dispatch-1"))

    verify(exactly = 0) { actionExecutor.execute(any()) }
  }
}
