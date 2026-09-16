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

  @Test
  fun `findAllById는 주어진 id들만 조회하고 빈 컬렉션이면 빈 리스트를 반환한다`() {
    val a = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING,
      ),
    )
    val b = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "t1", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING,
      ),
    )

    val result = adapter.findAllById(listOf(a.id, b.id))

    assertEquals(setOf(a.id, b.id), result.map { it.id }.toSet())
    assertEquals(emptyList(), adapter.findAllById(emptyList()))
  }

  @Test
  fun `findWaitingExpiredByTenant는 해당 테넌트의 만료 WAITING 인스턴스만 waitingUntil 오름차순으로 반환한다`() {
    val tenantId = "t-timeout-${System.nanoTime()}"
    val older = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = tenantId, triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(5),
      ),
    )
    val newer = adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = tenantId, triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
      ),
    )
    // 다른 테넌트의 만료 인스턴스는 섞이면 안 된다
    adapter.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(), workflowId = "wf-1", tenantId = "other-tenant", triggerEventCode = "CART_ADD",
        version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
      ),
    )

    val result = adapter.findWaitingExpiredByTenant(tenantId, LocalDateTime.now(), limit = 10)

    assertEquals(listOf(older.id, newer.id), result.map { it.id })
  }
}
