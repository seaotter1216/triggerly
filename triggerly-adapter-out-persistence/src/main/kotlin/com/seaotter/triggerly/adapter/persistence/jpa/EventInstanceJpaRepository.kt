package com.seaotter.triggerly.adapter.persistence.jpa

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.time.LocalDateTime

interface EventInstanceJpaRepository : JpaRepository<EventInstanceEntity, String> {
  @Modifying
  @Query("DELETE FROM EventInstanceEntity e WHERE e.occurredAt < :cutoff")
  fun deleteByOccurredAtBefore(cutoff: LocalDateTime): Int
}
