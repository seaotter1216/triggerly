package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.NodeType
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowExecutionStatus
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull

class WorkflowExecutionPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: WorkflowExecutionPersistenceAdapter

  @Test
  fun `findRunning은 RUNNING 상태인 실행 기록만 찾는다`() {
    val instanceId = UUID.randomUUID().toString()
    adapter.save(WorkflowExecution(UUID.randomUUID().toString(), instanceId, "n1", NodeType.TRIGGER, WorkflowExecutionStatus.COMPLETED))
    val running = adapter.save(WorkflowExecution(UUID.randomUUID().toString(), instanceId, "n2", NodeType.WAIT_FOR_EVENT, WorkflowExecutionStatus.RUNNING))

    val found = adapter.findRunning(instanceId, "n2")
    assertEquals(running.id, found?.id)
    assertNull(adapter.findRunning(instanceId, "n1"))
    assertEquals(2, adapter.findByInstanceId(instanceId).size)
  }
}
