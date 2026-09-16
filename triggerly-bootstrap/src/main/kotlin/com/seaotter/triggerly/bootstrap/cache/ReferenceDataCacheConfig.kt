package com.seaotter.triggerly.bootstrap.cache

import com.seaotter.triggerly.adapter.messaging.redis.EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL
import com.seaotter.triggerly.adapter.messaging.redis.WORKFLOW_CACHE_INVALIDATION_CHANNEL
import com.seaotter.triggerly.adapter.persistence.jpa.EventDefinitionPersistenceAdapter
import com.seaotter.triggerly.adapter.persistence.jpa.WorkflowPersistenceAdapter
import com.seaotter.triggerly.application.cache.CachedEventDefinitionAdapter
import com.seaotter.triggerly.application.cache.CachedWorkflowRepositoryAdapter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer

// EventDefinitionPersistenceAdapter/WorkflowPersistenceAdapter를 감싸 Caffeine으로 캐싱하는 데코레이터를
// @Primary로 등록한다 - EventIngestionController/IngestEventUseCase/TenantAwareTimeoutPoller는 여전히
// EventDefinitionPort/WorkflowRepositoryPort 인터페이스에만 의존하므로 이 배선을 전혀 몰라도 된다.
// 다른 앱 인스턴스에서 발행된 무효화 메시지를 받으려면 RedisMessageListenerContainer로 두 채널을 구독해
// 해당 데코레이터의 evict를 직접 호출한다 - 발행 인스턴스 자신도 같은 채널을 구독하므로 별도 로컬 경로
// 없이 항상 이 경로 하나로만 캐시가 비워진다.
@Configuration
class ReferenceDataCacheConfig {

  @Bean
  @Primary
  fun cachedEventDefinitionPort(delegate: EventDefinitionPersistenceAdapter): CachedEventDefinitionAdapter =
    CachedEventDefinitionAdapter(delegate)

  @Bean
  @Primary
  fun cachedWorkflowRepositoryPort(delegate: WorkflowPersistenceAdapter): CachedWorkflowRepositoryAdapter =
    CachedWorkflowRepositoryAdapter(delegate)

  @Bean
  fun cacheInvalidationListenerContainer(
    connectionFactory: RedisConnectionFactory,
    eventDefinitionCache: CachedEventDefinitionAdapter,
    workflowCache: CachedWorkflowRepositoryAdapter,
  ): RedisMessageListenerContainer {
    val container = RedisMessageListenerContainer()
    container.setConnectionFactory(connectionFactory)
    container.addMessageListener(
      MessageListener { message, _ ->
        val (tenantId, code) = String(message.body).split(":", limit = 2)
        eventDefinitionCache.evict(tenantId, code)
      },
      ChannelTopic(EVENT_DEFINITION_CACHE_INVALIDATION_CHANNEL),
    )
    container.addMessageListener(
      MessageListener { message, _ ->
        val (tenantId, workflowId) = String(message.body).split(":", limit = 2)
        workflowCache.evict(tenantId, workflowId)
      },
      ChannelTopic(WORKFLOW_CACHE_INVALIDATION_CHANNEL),
    )
    return container
  }
}
