package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.WorkflowInstance
import java.time.LocalDateTime

interface WorkflowInstanceRepositoryPort {
  fun save(instance: WorkflowInstance): WorkflowInstance
  fun findById(id: String): WorkflowInstance?

  // IngestEventUseCase.handleBatch가 배치 안의 대기 인스턴스 후보들을 개별 findById 대신 IN절 1회로
  // 조회하기 위한 벌크 메서드 (Task 8 참고).
  fun findAllById(ids: Collection<String>): List<WorkflowInstance>

  // TenantAwareTimeoutPoller(Task 9)가 테넌트별로 소량씩만 조회하기 위한 메서드. 전역 findWaitingExpired와
  // 달리 tenant_id를 필터에 포함해 (tenant_id, status, waiting_until) 인덱스를 그대로 타게 한다.
  fun findWaitingExpiredByTenant(tenantId: String, now: LocalDateTime, limit: Int): List<WorkflowInstance>
}
