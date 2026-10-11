package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.Workflow

interface WorkflowRepositoryPort {
  fun save(workflow: Workflow): Workflow
  fun findById(tenantId: String, id: String): Workflow?
  fun findAll(tenantId: String): List<Workflow>
  fun findEnabledByTriggerEventCode(tenantId: String, eventCode: String): List<Workflow>

  // TenantAwareTimeoutPoller(Task 9)가 "이번 틱에 순회할 테넌트 목록"의 소스로 쓴다 - workflow_instance
  // 대신 훨씬 작고 안정적인 workflow 테이블을 스캔한다.
  fun findDistinctTenantIds(): Set<String>

  // 관리자 UI에서 테스트용으로 만든 워크플로를 지울 수 있게 한다. 존재하지 않는 (tenantId, id)에 대해
  // false를 반환해 호출부가 404로 구분할 수 있게 한다.
  fun deleteById(tenantId: String, id: String): Boolean
}
