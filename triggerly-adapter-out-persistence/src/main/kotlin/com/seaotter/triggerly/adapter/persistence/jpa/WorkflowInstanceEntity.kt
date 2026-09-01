package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "workflow_instance")
class WorkflowInstanceEntity(
  @Id var id: String,
  var workflowId: String,
  var tenantId: String,
  var triggerEventCode: String,
  var version: Long,
  var memberId: String?,
  var status: String,
  var currentNodeId: String?,
  var waitingEventName: String?,
  var waitingUntil: LocalDateTime?,
  var startedAt: LocalDateTime?,
  var completedAt: LocalDateTime?,
)
