package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import java.time.LocalDateTime

interface WorkflowInstanceJpaRepository : JpaRepository<WorkflowInstanceEntity, String> {
  fun findAllByTenantIdAndStatusAndWaitingUntilLessThanEqualOrderByWaitingUntilAsc(
    tenantId: String,
    status: String,
    waitingUntil: LocalDateTime,
    pageable: Pageable,
  ): List<WorkflowInstanceEntity>

  // 관리자 UI가 인스턴스를 id 없이 테넌트 기준으로 훑어볼 수 있게 하는 목록 조회.
  // startedAt이 null인(아직 시작 전) 인스턴스는 거의 없지만 혹시 있으면 뒤로 밀리도록 Desc 정렬.
  fun findAllByTenantIdOrderByStartedAtDesc(tenantId: String, pageable: Pageable): List<WorkflowInstanceEntity>
}
