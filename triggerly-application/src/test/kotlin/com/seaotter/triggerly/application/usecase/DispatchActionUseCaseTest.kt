package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.domain.ActionDefinition
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class DispatchActionUseCaseTest {

  private val actionExecutor = mockk<ActionExecutor>(relaxed = true)
  private val useCase = DispatchActionUseCase(actionExecutor)

  @Test
  fun `handle은 메시지에 담긴 action을 그대로 ActionExecutor에 위임한다`() {
    val action = ActionDefinition.IssueCoupon("BIRTHDAY10")
    val message = ActionDispatchMessage(
      tenantId = "t1", memberId = "m1", action = action,
      workflowInstanceId = "wf-instance-1", nodeId = "n3", dispatchId = "dispatch-1",
    )

    useCase.handle(message)

    verify(exactly = 1) { actionExecutor.execute(action) }
  }
}
