package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.WaitingIndexPort
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisWaitingIndexAdapter(
  private val redisTemplate: StringRedisTemplate,
) : WaitingIndexPort {

  private fun key(tenantId: String, eventCode: String, memberId: String) =
    "waiting:$tenantId:event:$eventCode:member:$memberId"

  override fun register(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String) {
    redisTemplate.opsForList().rightPush(key(tenantId, eventCode, memberId), workflowInstanceId)
  }

  override fun remove(tenantId: String, eventCode: String, memberId: String, workflowInstanceId: String) {
    redisTemplate.opsForList().remove(key(tenantId, eventCode, memberId), 0, workflowInstanceId)
  }

  override fun lookup(tenantId: String, eventCode: String, memberId: String): List<String> =
    redisTemplate.opsForList().range(key(tenantId, eventCode, memberId), 0, -1) ?: emptyList()
}
