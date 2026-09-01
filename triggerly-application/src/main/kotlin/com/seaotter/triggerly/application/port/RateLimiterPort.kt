package com.seaotter.triggerly.application.port

fun interface RateLimiterPort {
  // true = 허용, false = 한도 초과
  fun tryConsume(tenantId: String): Boolean
}
