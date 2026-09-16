package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RawEventMessage
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.WorkflowInstance
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.Semaphore

// 스펙 6절 4단계: WAITING인데 waitingUntil이 지난 인스턴스를 찾아 타임아웃 합성 이벤트를 같은 Kafka
// 토픽에 다시 produce한다. 여러 앱 인스턴스가 리더 선출 없이 매 틱 동시에 폴링한다는 전제로:
//  1) 테넌트별로 만료 인스턴스를 조회하고(findWaitingExpiredByTenant, (tenant_id, status, waiting_until)
//     인덱스 사용), 제출 순서를 테넌트별 라운드로빈으로 인터리빙해 특정 테넌트의 백로그가 다른 테넌트를
//     굶기지 않게 한다.
//  2) 스레드 수는 테넌트 수와 무관하게 고정 크기 워커 풀 하나를 전 테넌트가 공유한다. 테넌트별
//     Semaphore로 "이 풀에서 동시에 처리 중인 이 테넌트의 작업 수"만 제한한다 - 테넌트가 늘어도
//     세마포어(카운터)만 늘 뿐 스레드/DB 커넥션은 늘지 않는다.
//  3) 여러 인스턴스가 같은 만료 row를 동시에 집었을 때는 Redis 행 단위 락(tryLock, TTL 경과 시 자동 해제)을
//     선점한 쪽만 실제로 합성 이벤트를 발행한다 - 실패하면 스킵한다(다른 인스턴스가 이미 처리했으므로
//     유실이 아니다).
@Component
class TenantAwareTimeoutPoller(
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val workflowRepositoryPort: WorkflowRepositoryPort,
  private val distributedLockPort: DistributedLockPort,
  private val eventPublisherPort: EventPublisherPort,
  @Value("\${triggerly.scheduler.timeout-poll-batch-size-per-tenant:200}") private val batchSizePerTenant: Int,
  @Value("\${triggerly.scheduler.timeout-poll-worker-pool-size:20}") private val workerPoolSize: Int,
  @Value("\${triggerly.scheduler.timeout-poll-per-tenant-concurrency:4}") private val perTenantConcurrency: Int,
  @Value("\${triggerly.scheduler.timeout-poll-lock-ttl-ms:30000}") private val lockTtlMs: Long,
) {
  private val log = LoggerFactory.getLogger(TenantAwareTimeoutPoller::class.java)
  private lateinit var workerPool: ExecutorService
  private val perTenantSemaphores = ConcurrentHashMap<String, Semaphore>()

  @PostConstruct
  fun start() {
    workerPool = Executors.newFixedThreadPool(workerPoolSize)
  }

  @PreDestroy
  fun stop() {
    workerPool.shutdown()
  }

  @Scheduled(fixedDelayString = "\${triggerly.scheduler.timeout-poll-interval-ms:10000}")
  fun pollExpiredInstances() {
    val tenantIds = workflowRepositoryPort.findDistinctTenantIds()
    if (tenantIds.isEmpty()) return

    val now = LocalDateTime.now()
    val backlogs = tenantIds
      .associateWith { tenantId -> workflowInstanceRepositoryPort.findWaitingExpiredByTenant(tenantId, now, batchSizePerTenant) }
      .filterValues { it.isNotEmpty() }
    if (backlogs.isEmpty()) return

    val futures = interleave(backlogs).map { instance -> workerPool.submit { processExpiredInstance(instance) } }
    waitForCompletion(futures)
  }

  // 테넌트1에서 1건, 테넌트2에서 1건, ... 한 바퀴 돌고 다시 테넌트1 - 공유 워커풀이 포화 상태여도
  // 뒷순번 테넌트가 굶지 않도록 제출 순서 자체를 섞는다.
  private fun interleave(backlogs: Map<String, List<WorkflowInstance>>): List<WorkflowInstance> {
    val queues = backlogs.values.map { it.toMutableList() }.filter { it.isNotEmpty() }.toMutableList()
    val result = mutableListOf<WorkflowInstance>()
    while (queues.isNotEmpty()) {
      val iterator = queues.iterator()
      while (iterator.hasNext()) {
        val queue = iterator.next()
        result += queue.removeAt(0)
        if (queue.isEmpty()) iterator.remove()
      }
    }
    return result
  }

  private fun processExpiredInstance(instance: WorkflowInstance) {
    val semaphore = perTenantSemaphores.computeIfAbsent(instance.tenantId) { Semaphore(perTenantConcurrency) }
    semaphore.acquire()
    try {
      if (!distributedLockPort.tryLock("timeout-lock:${instance.id}", Duration.ofMillis(lockTtlMs))) {
        return // 다른 인스턴스가 이미 처리 중 - 스킵(유실 아님, 처리 안 되면 다음 틱까지 그대로 남아있음)
      }
      log.info("타임아웃 감지: instanceId=${instance.id} tenantId=${instance.tenantId} node=${instance.currentNodeId}")
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
    } finally {
      semaphore.release()
    }
  }

  private fun waitForCompletion(futures: List<Future<*>>) {
    futures.forEach { future ->
      try {
        future.get()
      } catch (e: Exception) {
        log.error("타임아웃 인스턴스 처리 중 예외 발생", e)
      }
    }
  }
}
