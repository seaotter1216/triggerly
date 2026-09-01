package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.domain.EventInstance
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Component
class EventInstanceMySqlStore(private val repository: EventInstanceJpaRepository) {

  fun save(instance: EventInstance): EventInstance {
    val entity = EventInstanceEntity(
      id = instance.id, tenantId = instance.tenantId, eventCode = instance.eventCode,
      occurredAt = instance.occurredAt, memberId = instance.memberId, attributes = instance.attributes,
    )
    return repository.save(entity).toDomain()
  }

  @Transactional
  fun deleteOlderThan(cutoff: LocalDateTime): Int = repository.deleteByOccurredAtBefore(cutoff)

  private fun EventInstanceEntity.toDomain() = EventInstance(id, tenantId, eventCode, occurredAt, memberId, attributes)
}
