package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.DistributedLockPort
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RedisDistributedLockAdapter(
  private val redisTemplate: StringRedisTemplate,
) : DistributedLockPort {

  private val log = LoggerFactory.getLogger(RedisDistributedLockAdapter::class.java)

  // Redis 장애 시 fail-open: 락을 못 얻은 것으로 취급해 호출자가 그냥 스킵하게 한다 - 예외를 던져
  // 처리 전체를 막지 않는다(타임아웃 폴러/액션 디스패치 둘 다 이 메서드가 예외를 던지면 배치 전체가
  // 실패로 처리되므로, Redis 순단 하나로 전체 처리가 막히는 것을 피한다).
  override fun tryLock(key: String, ttl: Duration): Boolean =
    try {
      redisTemplate.opsForValue().setIfAbsent(key, "1", ttl) ?: false
    } catch (e: DataAccessException) {
      log.warn("Redis 락 획득 실패(fail-open으로 처리): key={}", key, e)
      false
    }
}
