package com.seaotter.triggerly.adapter.persistence

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.ThreadPoolExecutor

@Configuration
@EnableAsync
class AsyncConfig {

  @Bean("memberSyncExecutor")
  fun memberSyncExecutor(
    @Value("\${triggerly.async.member-sync.core-pool-size:4}") corePoolSize: Int,
    @Value("\${triggerly.async.member-sync.max-pool-size:16}") maxPoolSize: Int,
    @Value("\${triggerly.async.member-sync.queue-capacity:1000}") queueCapacity: Int,
  ): ThreadPoolTaskExecutor {
    val executor = ThreadPoolTaskExecutor()
    executor.corePoolSize = corePoolSize
    executor.maxPoolSize = maxPoolSize
    executor.setQueueCapacity(queueCapacity)
    executor.setThreadNamePrefix("member-sync-")
    executor.setRejectedExecutionHandler(ThreadPoolExecutor.CallerRunsPolicy())
    executor.initialize()
    return executor
  }
}
