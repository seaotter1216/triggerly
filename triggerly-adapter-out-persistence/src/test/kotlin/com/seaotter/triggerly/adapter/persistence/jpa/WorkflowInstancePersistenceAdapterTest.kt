package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class WorkflowInstancePersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: WorkflowInstancePersistenceAdapter

  @Test
  fun `findWaitingExpired는 waitingUntil이 지난 WAITING 인스턴스만 반환한다`() {
    val expired = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
      ),
    )
    adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().plusMinutes(10),
      ),
    )

    val result = adapter.findWaitingExpired(LocalDateTime.now())

    assertEquals(1, result.size)
    assertEquals(expired.id, result[0].id)
  }
}
