package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import java.time.LocalDate

@Document(indexName = "member")
class MemberDocument(
  @Id var id: String,
  var tenantId: String,
  var externalMemberId: String,
  var name: String?,
  var email: String?,
  var telephone: String?,
  var devicePlatform: String?,
  var gender: String?,
  @Field(type = FieldType.Date, format = [org.springframework.data.elasticsearch.annotations.DateFormat.date])
  var birthday: LocalDate?,
  var status: String?,
  @Field(type = FieldType.Object)
  var attributes: Map<String, Any?>?,
)
