package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import java.time.LocalDateTime

interface WorkflowInstanceJpaRepository : JpaRepository<WorkflowInstanceEntity, String> {
  fun findAllByStatusAndWaitingUntilLessThanEqual(
    status: String,
    waitingUntil: LocalDateTime,
    pageable: Pageable,
  ): List<WorkflowInstanceEntity>

  fun findAllByTenantIdAndStatusAndWaitingUntilLessThanEqualOrderByWaitingUntilAsc(
    tenantId: String,
    status: String,
    waitingUntil: LocalDateTime,
    pageable: Pageable,
  ): List<WorkflowInstanceEntity>
}
