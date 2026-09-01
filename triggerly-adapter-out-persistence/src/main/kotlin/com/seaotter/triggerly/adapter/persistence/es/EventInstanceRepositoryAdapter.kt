package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.jpa.EventInstanceMySqlStore
import com.seaotter.triggerly.application.port.EventInstanceRepositoryPort
import com.seaotter.triggerly.domain.EventInstance
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class EventInstanceRepositoryAdapter(
  private val mysqlStore: EventInstanceMySqlStore,
  private val esRepository: EventLogElasticsearchRepository,
) : EventInstanceRepositoryPort {

  override fun save(instance: EventInstance): EventInstance {
    val saved = mysqlStore.save(instance)
    esRepository.save(saved.toDocument())
    return saved
  }

  override fun deleteOlderThan(cutoff: LocalDateTime): Int = mysqlStore.deleteOlderThan(cutoff)

  private fun EventInstance.toDocument() = EventLogDocument(id, tenantId, eventCode, occurredAt, memberId, attributes)
}
