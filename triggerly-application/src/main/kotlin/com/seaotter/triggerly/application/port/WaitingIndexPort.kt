package com.seaotter.triggerly.application.port

interface WaitingIndexPort {
  fun register(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String)
  fun remove(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String)
  fun lookup(tenantId: String, eventCode: String, memberId: String): List<String>
}
