package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.WorkflowExecution
import com.seaotter.triggerly.domain.WorkflowInstance
import org.springframework.stereotype.Service

data class WorkflowInstanceView(val instance: WorkflowInstance, val executions: List<WorkflowExecution>)

@Service
class WorkflowInstanceQueryUseCase(
  private val instancePort: WorkflowInstanceRepositoryPort,
  private val executionPort: WorkflowExecutionRepositoryPort,
) {
  fun get(id: String): WorkflowInstanceView? {
    val instance = instancePort.findById(id) ?: return null
    return WorkflowInstanceView(instance, executionPort.findByInstanceId(id))
  }

  // 관리자 UI(인스턴스 탐색 탭)용 목록 조회. workflowId가 주어지면 애플리케이션 레벨에서 필터링한다 -
  // 테넌트당 인스턴스 수가 적은 관리자/테스트 용도라 별도 쿼리 메서드를 새로 만들 필요는 없다.
  fun list(tenantId: String, workflowId: String? = null, limit: Int = 50): List<WorkflowInstance> {
    val instances = instancePort.findByTenant(tenantId, limit)
    return if (workflowId == null) instances else instances.filter { it.workflowId == workflowId }
  }
}
