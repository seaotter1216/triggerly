package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import java.time.LocalDateTime

@Document(indexName = "event_log")
class EventLogDocument(
  @Id var id: String,
  @Field(type = FieldType.Keyword) var tenantId: String,
  @Field(type = FieldType.Keyword) var eventCode: String,
  @Field(type = FieldType.Date) var occurredAt: LocalDateTime,
  @Field(type = FieldType.Keyword) var memberId: String?,
  @Field(type = FieldType.Object) var attributes: Map<String, Any?>?,
)
