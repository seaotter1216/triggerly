package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "event_definition")
class EventDefinitionEntity(
  @Id var id: String,
  var tenantId: String,
  var code: String,
  var displayName: String,
  var createdAt: LocalDateTime,
)
