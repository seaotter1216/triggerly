package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.WorkflowInstance
import java.time.LocalDateTime

interface WorkflowInstanceRepositoryPort {
  fun save(instance: WorkflowInstance): WorkflowInstance
  fun findById(id: String): WorkflowInstance?
  fun findWaitingExpired(now: LocalDateTime, limit: Int = 200): List<WorkflowInstance>
}
