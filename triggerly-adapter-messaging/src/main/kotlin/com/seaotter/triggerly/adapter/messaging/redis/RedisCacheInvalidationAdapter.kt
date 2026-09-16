package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

const val EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL = "cache:invalidate:event-definition"
const val WORKFLOW_CACHE_INVALIDATION_CHANNEL = "cache:invalidate:workflow"

@Component
class RedisCacheInvalidationAdapter(
  private val redisTemplate: StringRedisTemplate,
) : CacheInvalidationPort {

  private val log = LoggerFactory.getLogger(RedisCacheInvalidationAdapter::class.java)

  // Redis 장애 시 fail-open: 무효화 발행이 실패해도 저장 자체를 실패시키지 않는다 - 놓친 무효화는
  // 캐시 데코레이터의 60초 TTL 안전망이 대신 처리한다(스펙 참고).
  override fun publish(topic: CacheInvalidationTopic, key: String) {
    val channel = when (topic) {
      CacheInvalidationTopic.EVENT_DEFINITION -> EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL
      CacheInvalidationTopic.WORKFLOW -> WORKFLOW_CACHE_INVALIDATION_CHANNEL
    }
    try {
      redisTemplate.convertAndSend(channel, key)
    } catch (e: DataAccessException) {
      log.warn("캐시 무효화 발행 실패(TTL 안전망에 위임): topic={} key={}", topic, key, e)
    }
  }
}
