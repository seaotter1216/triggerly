package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MySqlIntegrationTest
import com.seaotter.triggerly.domain.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkflowPersistenceAdapterTest : MySqlIntegrationTest() {

  @Autowired
  lateinit var adapter: WorkflowPersistenceAdapter

  private fun sampleWorkflow(id: String, status: WorkflowStatus) = Workflow(
    id = id, tenantId = "t1", triggerEventCode = "LOGIN",
    definitionJson = WorkflowDefinition("LOGIN", listOf(Node.Trigger("n1", "LOGIN"), Node.End("n2")), listOf(Edge("n1", "n2", EdgeRoute.Always))),
    status = status, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )

  @Test
  fun `definitionJson을 저장하고 복원하면 노드-엣지 구조가 그대로 유지된다`() {
    adapter.save(sampleWorkflow("wf-1", WorkflowStatus.DRAFT))
    val found = adapter.findById("t1", "wf-1")
    assertEquals(2, found?.definitionJson?.nodes?.size)
    assertTrue(found?.definitionJson?.nodes?.get(0) is Node.Trigger)
  }

  @Test
  fun `findEnabledByTriggerEventCode는 ENABLED 상태만 반환한다`() {
    adapter.save(sampleWorkflow("wf-2", WorkflowStatus.ENABLED))
    adapter.save(sampleWorkflow("wf-3", WorkflowStatus.DRAFT))
    val enabled = adapter.findEnabledByTriggerEventCode("t1", "LOGIN")
    assertEquals(1, enabled.size)
    assertEquals("wf-2", enabled[0].id)
  }

  @Test
  fun `findDistinctTenantIds는 워크플로가 존재하는 테넌트를 중복 없이 반환한다`() {
    val tenantA = "t-distinct-a-${System.nanoTime()}"
    val tenantB = "t-distinct-b-${System.nanoTime()}"
    adapter.save(sampleWorkflow("wf-distinct-1", WorkflowStatus.ENABLED).let {
      Workflow(id = it.id, tenantId = tenantA, triggerEventCode = it.triggerEventCode, definitionJson = it.definitionJson, status = it.status, createdAt = it.createdAt, lastUpdatedAt = it.lastUpdatedAt)
    })
    adapter.save(sampleWorkflow("wf-distinct-2", WorkflowStatus.DRAFT).let {
      Workflow(id = it.id, tenantId = tenantA, triggerEventCode = it.triggerEventCode, definitionJson = it.definitionJson, status = it.status, createdAt = it.createdAt, lastUpdatedAt = it.lastUpdatedAt)
    })
    adapter.save(sampleWorkflow("wf-distinct-3", WorkflowStatus.ENABLED).let {
      Workflow(id = it.id, tenantId = tenantB, triggerEventCode = it.triggerEventCode, definitionJson = it.definitionJson, status = it.status, createdAt = it.createdAt, lastUpdatedAt = it.lastUpdatedAt)
    })

    val result = adapter.findDistinctTenantIds()

    assertTrue(result.containsAll(setOf(tenantA, tenantB)))
  }
}
