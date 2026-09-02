package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.RateLimiterPort
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class RedisRateLimiterAdapter(
  private val redisTemplate: StringRedisTemplate,
  @Value("\${triggerly.ratelimit.default-rps:2000}") private val defaultRps: Int,
) : RateLimiterPort {

  override fun tryConsume(tenantId: String): Boolean {
    val windowSecond = Instant.now().epochSecond
    val key = "ratelimit:$tenantId:$windowSecond"
    // ARGV[2]=2초 TTL — 윈도우가 끝난 직후 들어오는 클럭 오차를 흡수하기 위한 여유
    val result = redisTemplate.execute(RateLimitScript.INSTANCE, listOf(key), defaultRps.toString(), "2")
    return result == 1L
  }
}
