package com.seaotter.triggerly.adapter.messaging.redis

import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.application.port.LockResult
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

  // Redis 장애(커넥션 실패 등)는 "이미 락이 잡혀있다"와 구분해 Unavailable로 반환한다 - 호출부가
  // 이 둘을 다르게 처리할 수 있게 하기 위함이다(DispatchActionUseCase 참고). 예외를 던지지는 않는다 -
  // 타임아웃 폴러는 어느 쪽이든 스킵하면 되므로 예외 처리 강제할 필요가 없다.
  override fun tryLock(key: String, ttl: Duration): LockResult =
    try {
      if (redisTemplate.opsForValue().setIfAbsent(key, "1", ttl) == true) LockResult.Acquired else LockResult.AlreadyHeld
    } catch (e: DataAccessException) {
      log.warn("Redis 락 획득 실패(장애로 판단): key={}", key, e)
      LockResult.Unavailable
    }

  // Redis 장애로 삭제가 실패해도 예외를 던지지 않는다 - release는 "다음 재시도를 허용하기 위한 정리"
  // 목적이라, 여기서 실패해도 원래 예외(액션 실행 실패)를 가리면 안 되기 때문이다. 최악의 경우 TTL이
  // 지나면 어차피 자연 해제된다.
  override fun release(key: String) {
    try {
      redisTemplate.delete(key)
    } catch (e: DataAccessException) {
      log.warn("Redis 락 해제 실패(TTL 자연 만료에 위임): key={}", key, e)
    }
  }
}
