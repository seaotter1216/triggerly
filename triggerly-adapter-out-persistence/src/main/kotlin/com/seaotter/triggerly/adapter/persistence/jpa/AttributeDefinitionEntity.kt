package com.seaotter.triggerly.adapter.persistence.jpa

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "attribute_definition")
class AttributeDefinitionEntity(
  @Id var id: String,
  var tenantId: String,
  var eventDefinitionId: String?,
  var attrKey: String,
  var displayName: String,
  var attrType: String,
  var filterable: Boolean,
  var createdAt: LocalDateTime,
)
