package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import com.seaotter.triggerly.domain.WorkflowInstanceStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertTrue

class TenantAwareTimeoutPollerTest {

  private val workflowInstanceRepositoryPort = mockk<WorkflowInstanceRepositoryPort>()
  private val workflowRepositoryPort = mockk<WorkflowRepositoryPort>()
  private val distributedLockPort = mockk<DistributedLockPort>()
  private val eventPublisherPort = mockk<EventPublisherPort>(relaxed = true)

  private fun poller(perTenantConcurrency: Int = 4, poolSize: Int = 20) =
    TenantAwareTimeoutPoller(
      workflowInstanceRepositoryPort, workflowRepositoryPort, distributedLockPort, eventPublisherPort,
      batchSizePerTenant = 200, workerPoolSize = poolSize, perTenantConcurrency = perTenantConcurrency, lockTtlMs = 30000,
    ).also { it.start() }

  private fun instance(id: String, tenantId: String) = WorkflowInstance(
    id = id, workflowId = "wf-1", tenantId = tenantId, triggerEventCode = "CART_ADD",
    version = 1, status = WorkflowInstanceStatus.WAITING, waitingUntil = LocalDateTime.now().minusMinutes(1),
  )

  @Test
  fun `테넌트가 없으면 아무 것도 조회하지 않는다`() {
    every { workflowRepositoryPort.findDistinctTenantIds() } returns emptySet()
    val sut = poller()

    sut.pollExpiredInstances()

    verify(exactly = 0) { workflowInstanceRepositoryPort.findWaitingExpiredByTenant(any(), any(), any()) }
    sut.stop()
  }

  @Test
  fun `락을 선점하면 타임아웃 합성 이벤트를 발행하고, 이미 잠겨있으면 스킵한다`() {
    every { workflowRepositoryPort.findDistinctTenantIds() } returns setOf("t1")
    every { workflowInstanceRepositoryPort.findWaitingExpiredByTenant("t1", any(), any()) } returns
      listOf(instance("i1", "t1"), instance("i2", "t1"))
    every { distributedLockPort.tryLock("timeout-lock:i1", any()) } returns true
    every { distributedLockPort.tryLock("timeout-lock:i2", any()) } returns false
    val sut = poller()

    sut.pollExpiredInstances()

    verify(exactly = 1) { eventPublisherPort.publish(match { it.syntheticTimeoutForInstanceId == "i1" }) }
    verify(exactly = 0) { eventPublisherPort.publish(match { it.syntheticTimeoutForInstanceId == "i2" }) }
    sut.stop()
  }

  @Test
  fun `한 테넌트의 동시 처리 건수는 설정된 per-tenant-concurrency를 넘지 않는다`() {
    val inFlight = AtomicInteger(0)
    val maxObserved = AtomicInteger(0)
    every { workflowRepositoryPort.findDistinctTenantIds() } returns setOf("busy-tenant")
    every { workflowInstanceRepositoryPort.findWaitingExpiredByTenant("busy-tenant", any(), any()) } returns
      (1..50).map { instance("i$it", "busy-tenant") }
    every { distributedLockPort.tryLock(any(), any()) } answers {
      val current = inFlight.incrementAndGet()
      maxObserved.updateAndGet { prev -> maxOf(prev, current) }
      Thread.sleep(20)
      inFlight.decrementAndGet()
      true
    }
    val sut = poller(perTenantConcurrency = 4, poolSize = 20)

    sut.pollExpiredInstances()

    assertTrue(
      maxObserved.get() in 2..4,
      "관측된 최대 동시 처리 건수(${maxObserved.get()})는 설정값(4) 이하이면서 실제 동시성이 있었어야 한다",
    )
    sut.stop()
  }
}
