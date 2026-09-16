package com.seaotter.triggerly.application.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Ticker
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import java.time.Duration
import java.util.Optional

// EventIngestionController가 이벤트 하나를 받을 때마다 EventDefinitionPort.findByTenantAndCode를
// 캐시 없이 호출하던 것을 없애기 위한 read-through 캐시. save/findAll(관리자 CRUD 경로, 호출 빈도가
// 낮음)은 캐시하지 않고 delegate에 그대로 위임한다. EventDefinition.displayName은 var라 이론적으로는
// 가변이지만, 이 코드베이스에서 EventDefinitionPort를 읽었다가 그대로 mutate해서 되돌려 쓰는 호출부가
// 없어(ManageEventDefinitionUseCase.register는 매번 새 인스턴스를 만들어 저장) 방어적 복사가 필요 없다.
// evict()는 Redis pub/sub 무효화 메시지를 받았을 때 호출된다(ReferenceDataCacheConfig, Task 6 참고) -
// 이 클래스 자신은 Redis를 전혀 모른다.
class CachedEventDefinitionAdapter(
  private val delegate: EventDefinitionPort,
  ttl: Duration = Duration.ofSeconds(60),
  ticker: Ticker = Ticker.systemTicker(),
) : EventDefinitionPort {

  private val cache = Caffeine.newBuilder()
    .expireAfterWrite(ttl)
    .ticker(ticker)
    .build<String, Optional<EventDefinition>>()

  override fun save(definition: EventDefinition): EventDefinition = delegate.save(definition)

  override fun findByTenantAndCode(tenantId: String, code: String): EventDefinition? =
    cache.get(cacheKey(tenantId, code)) { Optional.ofNullable(delegate.findByTenantAndCode(tenantId, code)) }
      .orElse(null)

  override fun findAll(tenantId: String): List<EventDefinition> = delegate.findAll(tenantId)

  fun evict(tenantId: String, code: String) {
    cache.invalidate(cacheKey(tenantId, code))
  }

  private fun cacheKey(tenantId: String, code: String) = "$tenantId:$code"
}
