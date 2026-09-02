package com.seaotter.triggerly.adapter.messaging.redis

import org.springframework.data.redis.core.script.DefaultRedisScript

object RateLimitScript {
  private const val SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
  return 0
else
  return 1
end
"""

  val INSTANCE: DefaultRedisScript<Long> = DefaultRedisScript(SCRIPT, Long::class.java)
}
