package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.json.WorkflowDefinitionJsonConverter
import com.seaotter.triggerly.domain.WorkflowDefinition
import jakarta.persistence.Column
import jakarta.persistence.Convert
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "workflow")
class WorkflowEntity(
  @Id var id: String,
  var tenantId: String,
  var name: String?,
  var triggerEventCode: String,
  @Column(columnDefinition = "JSON")
  @Convert(converter = WorkflowDefinitionJsonConverter::class)
  var definitionJson: WorkflowDefinition,
  var version: Long,
  var status: String,
  var createdAt: LocalDateTime,
  var lastUpdatedAt: LocalDateTime,
)
