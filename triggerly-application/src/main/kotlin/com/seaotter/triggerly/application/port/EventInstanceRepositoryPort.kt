package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.EventInstance
import java.time.LocalDateTime

interface EventInstanceRepositoryPort {
  fun save(instance: EventInstance): EventInstance
  fun saveAll(instances: Collection<EventInstance>): List<EventInstance>
  fun deleteOlderThan(cutoff: LocalDateTime): Int
}
