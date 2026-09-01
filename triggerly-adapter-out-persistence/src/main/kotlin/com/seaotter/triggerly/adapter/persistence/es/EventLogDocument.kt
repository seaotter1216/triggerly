package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import java.time.LocalDateTime

@Document(indexName = "event_log")
class EventLogDocument(
  @Id var id: String,
  var tenantId: String,
  var eventCode: String,
  @Field(type = FieldType.Date) var occurredAt: LocalDateTime,
  var memberId: String?,
  @Field(type = FieldType.Object) var attributes: Map<String, Any?>?,
)
