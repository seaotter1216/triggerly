package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import java.time.LocalDate

@Document(indexName = "member")
class MemberDocument(
  @Id var id: String,
  @Field(type = FieldType.Keyword) var tenantId: String,
  @Field(type = FieldType.Keyword) var externalMemberId: String,
  var name: String?,
  var email: String?,
  var telephone: String?,
  @Field(type = FieldType.Keyword) var devicePlatform: String?,
  @Field(type = FieldType.Keyword) var gender: String?,
  @Field(type = FieldType.Date, format = [org.springframework.data.elasticsearch.annotations.DateFormat.date])
  var birthday: LocalDate?,
  @Field(type = FieldType.Keyword) var status: String?,
  @Field(type = FieldType.Object)
  var attributes: Map<String, Any?>?,
)
