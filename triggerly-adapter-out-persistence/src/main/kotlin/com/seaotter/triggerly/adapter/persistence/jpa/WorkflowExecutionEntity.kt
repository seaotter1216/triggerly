package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "workflow_execution")
class WorkflowExecutionEntity(
  @Id var id: String,
  var workflowInstanceId: String,
  var nodeId: String,
  var nodeType: String,
  var status: String,
  var startedAt: LocalDateTime?,
  var completedAt: LocalDateTime?,
  var result: String?,
  var errorCode: String?,
)
