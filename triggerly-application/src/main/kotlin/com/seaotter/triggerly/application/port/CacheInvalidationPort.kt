package com.seaotter.triggerly.application.port

enum class CacheInvalidationTopic { EVENT_DEFINITION, WORKFLOW }

// 이 인스턴스가 참조 데이터를 저장한 뒤, 다른 모든 앱 인스턴스(자기 자신 포함)의 캐시를 비우라고
// 알리기 위한 포트. key는 캐시 데코레이터가 evict()에 그대로 넘길 수 있는 형식("tenantId:code" 등)이다.
fun interface CacheInvalidationPort {
  fun publish(topic: CacheInvalidationTopic, key: String)
}
