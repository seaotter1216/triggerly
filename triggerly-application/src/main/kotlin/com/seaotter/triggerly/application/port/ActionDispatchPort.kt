package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.ActionDefinition

// Action 노드 실행을 fire-and-forget으로 비동기 처리하기 위한 메시지. WorkflowEngine은 이걸 발행만 하고
// 바로 다음 노드로 진행한다 - 실제 프로바이더 호출(과 그 지연/장애)은 별도 컨슈머가 완전히 독립된 컨슈머
// 그룹/스레드 풀에서 처리하므로, raw-events 수집 컨슈머 스레드를 절대 막지 않는다.
data class ActionDispatchMessage(
  val tenantId: String,
  val memberId: String?,
  val action: ActionDefinition,
  val workflowInstanceId: String,
  val nodeId: String,
  // 프로바이더 API 호출 시 idempotency key로 넘기기 위한 값. dispatch 토픽 자체의 재배달(카프카는
  // at-least-once까지만 보장)로 같은 메시지가 두 번 소비돼도, 프로바이더가 이 값을 멱등키로 지원하면
  // 실제 발송은 한 번만 일어난다. WorkflowEngine이 (workflowInstanceId, nodeId)로 결정론적으로 만들어
  // 재시도 때도 항상 같은 값이다 - eventId+workflowId로 인스턴스 id를 고정한 것과 같은 패턴.
  val dispatchId: String,
)

fun interface ActionDispatchPort {
  fun publish(message: ActionDispatchMessage)
}
