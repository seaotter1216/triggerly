package com.seaotter.triggerly.application.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker
import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Workflow
import java.time.Duration
import java.util.Optional

// EventIngestionController/IngestEventUseCase/TenantAwareTimeoutPoller가 워크플로 정의·테넌트 목록을
// 반복 조회하는 것을 막기 위한 read-through 캐시. save/findAll(관리자 CRUD, 저빈도)은 캐시하지 않는다.
class CachedWorkflowRepositoryAdapter(
  private val delegate: WorkflowRepositoryPort,
  ttl: Duration = Duration.ofSeconds(60),
  ticker: Ticker = Ticker.systemTicker(),
) : WorkflowRepositoryPort {

  private val byIdCache = Caffeine.newBuilder().expireAfterWrite(ttl).ticker(ticker)
    .build<String, Optional<Workflow>>()
  private val byTriggerCache = Caffeine.newBuilder().expireAfterWrite(ttl).ticker(ticker)
    .build<String, List<Workflow>>()
  private val tenantIdsCache = Caffeine.newBuilder().expireAfterWrite(ttl).ticker(ticker)
    .build<String, Set<String>>()

  override fun save(workflow: Workflow): Workflow = delegate.save(workflow)

  override fun findAll(tenantId: String): List<Workflow> = delegate.findAll(tenantId)

  // Workflow는 status/lastUpdatedAt이 var인 가변 클래스다. ManageWorkflowUseCase.enable()이 findById로
  // 받은 인스턴스를 그대로 mutate한 뒤 save()에 넘기므로(read-modify-write), 캐시에 저장된 객체를 그대로
  // 반환하면 save 전에 이미 캐시가 오염된다. 그래서 캐시 히트/미스와 무관하게 항상 새 인스턴스를 복사해
  // 반환한다 - 호출자가 뭘 하든 캐시 내부 상태는 절대 바뀌지 않는다.
  override fun findById(tenantId: String, id: String): Workflow? =
    byIdCache.get(idKey(tenantId, id)) { Optional.ofNullable(delegate.findById(tenantId, id)) }
      .orElse(null)
      ?.let(::copyOf)

  // 현재 호출부(IngestEventUseCase)는 반환된 Workflow를 읽기만 하고 mutate하지 않으므로 방어적 복사가
  // 당장은 필요 없다 - 나중에 mutate하는 호출부가 생기면 findById와 동일하게 copyOf를 적용해야 한다.
  override fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow> =
    byTriggerCache.get(triggerKey(tenantId, eventCode)) { delegate.findEnabledByTriggerEventCode(tenantId, eventCode) }

  override fun findDistinctTenantIds(): Set<String> = tenantIdsCache.get(TENANT_IDS_KEY) { delegate.findDistinctTenantIds() }

  fun evict(tenantId: String, workflowId: String) {
    byIdCache.invalidate(idKey(tenantId, workflowId))
    // 워크플로 하나가 바뀌면 그게 어떤 triggerEventCode에 걸려 있었는지 이 클래스는 알 수 없으므로(evict
    // 호출 시점엔 tenantId+workflowId만 전달됨) 안전하게 그 테넌트의 트리거 캐시 전체와 테넌트 목록
    // 캐시를 함께 비운다 - 워크플로 변경은 admin 경로에서만 드물게 발생해 과잉 무효화 비용이 작다.
    byTriggerCache.asMap().keys.removeIf { it.startsWith("$tenantId:") }
    tenantIdsCache.invalidateAll()
  }

  private fun copyOf(workflow: Workflow) = Workflow(
    id = workflow.id, tenantId = workflow.tenantId, name = workflow.name, triggerEventCode = workflow.triggerEventCode,
    definitionJson = workflow.definitionJson, version = workflow.version, status = workflow.status,
    createdAt = workflow.createdAt, lastUpdatedAt = workflow.lastUpdatedAt,
  )

  private fun idKey(tenantId: String, id: String) = "$tenantId:$id"
  private fun triggerKey(tenantId: String, eventCode: String) = "$tenantId:$eventCode"

  companion object {
    private const val TENANT_IDS_KEY = "__tenants__"
  }
}
