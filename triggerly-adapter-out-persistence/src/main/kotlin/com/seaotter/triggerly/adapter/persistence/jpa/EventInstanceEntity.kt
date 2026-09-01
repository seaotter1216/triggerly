package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.json.MapJsonConverter
import jakarta.persistence.Column
import jakarta.persistence.Convert
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "event_instance")
class EventInstanceEntity(
  @Id var id: String,
  var tenantId: String,
  var eventCode: String,
  var occurredAt: LocalDateTime,
  var memberId: String?,
  @Column(columnDefinition = "JSON")
  @Convert(converter = MapJsonConverter::class)
  var attributes: Map<String, Any?>?,
)
