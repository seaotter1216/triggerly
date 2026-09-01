package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository

interface WorkflowExecutionJpaRepository : JpaRepository<WorkflowExecutionEntity, String> {
  fun findAllByWorkflowInstanceId(workflowInstanceId: String): List<WorkflowExecutionEntity>
  fun findFirstByWorkflowInstanceIdAndNodeIdAndStatus(
    workflowInstanceId: String,
    nodeId: String,
    status: String,
  ): WorkflowExecutionEntity?
}
