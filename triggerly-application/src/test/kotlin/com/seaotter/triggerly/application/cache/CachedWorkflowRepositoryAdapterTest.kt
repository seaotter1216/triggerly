package com.seaotter.triggerly.application.cache

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Edge
import com.seaotter.triggerly.domain.EdgeRoute
import com.seaotter.triggerly.domain.Node
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame

class CachedWorkflowRepositoryAdapterTest {

  private val delegate = mockk<WorkflowRepositoryPort>()

  private fun sampleWorkflow() = Workflow(
    id = "wf-1", tenantId = "t1", triggerEventCode = "LOGIN",
    definitionJson = WorkflowDefinition("LOGIN", listOf(Node.Trigger("n1", "LOGIN"), Node.End("n2")), listOf(Edge("n1", "n2", EdgeRoute.Always))),
    status = WorkflowStatus.DRAFT, createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
  )

  @Test
  fun `findById를 반복 호출해도 delegate는 한 번만 호출된다`() {
    every { delegate.findById("t1", "wf-1") } returns sampleWorkflow()
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    repeat(3) { cache.findById("t1", "wf-1") }

    verify(exactly = 1) { delegate.findById("t1", "wf-1") }
  }

  @Test
  fun `findById가 반환한 객체를 호출자가 mutate해도 캐시된 값에는 영향이 없다`() {
    every { delegate.findById("t1", "wf-1") } returns sampleWorkflow()
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    val first = cache.findById("t1", "wf-1")!!
    assertEquals(WorkflowStatus.DRAFT, first.status)
    first.status = WorkflowStatus.ENABLED // ManageWorkflowUseCase.enable()과 동일한 read-modify-write

    val second = cache.findById("t1", "wf-1")!!
    assertNotSame(first, second)
    assertEquals(WorkflowStatus.DRAFT, second.status)
  }

  @Test
  fun `findEnabledByTriggerEventCode를 반복 호출해도 delegate는 한 번만 호출된다`() {
    every { delegate.findEnabledByTriggerEventCode("t1", "LOGIN") } returns listOf(sampleWorkflow())
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    repeat(3) { cache.findEnabledByTriggerEventCode("t1", "LOGIN") }

    verify(exactly = 1) { delegate.findEnabledByTriggerEventCode("t1", "LOGIN") }
  }

  @Test
  fun `findDistinctTenantIds를 반복 호출해도 delegate는 한 번만 호출된다`() {
    every { delegate.findDistinctTenantIds() } returns setOf("t1", "t2")
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    repeat(3) { cache.findDistinctTenantIds() }

    verify(exactly = 1) { delegate.findDistinctTenantIds() }
  }

  @Test
  fun `evict 이후에는 findById가 delegate를 다시 호출한다`() {
    every { delegate.findById("t1", "wf-1") } returns sampleWorkflow()
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    cache.findById("t1", "wf-1")
    cache.evict("t1", "wf-1")
    cache.findById("t1", "wf-1")

    verify(exactly = 2) { delegate.findById("t1", "wf-1") }
  }

  @Test
  fun `save와 findAll은 캐시를 거치지 않고 delegate에 그대로 위임한다`() {
    every { delegate.findAll("t1") } returns emptyList()
    val saved = sampleWorkflow()
    every { delegate.save(any()) } returns saved
    val cache = CachedWorkflowRepositoryAdapter(delegate)

    cache.findAll("t1")
    cache.save(saved)

    verify(exactly = 1) { delegate.findAll("t1") }
    verify(exactly = 1) { delegate.save(saved) }
  }
}
