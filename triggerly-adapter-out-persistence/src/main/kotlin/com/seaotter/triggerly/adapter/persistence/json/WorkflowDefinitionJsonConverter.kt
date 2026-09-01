package com.seaotter.triggerly.adapter.persistence.json

import com.fasterxml.jackson.module.kotlin.readValue
import com.seaotter.triggerly.domain.WorkflowDefinition
import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter

@Converter
class WorkflowDefinitionJsonConverter : AttributeConverter<WorkflowDefinition, String> {
  override fun convertToDatabaseColumn(attribute: WorkflowDefinition): String =
    JsonMapper.instance.writeValueAsString(attribute)

  override fun convertToEntityAttribute(dbData: String): WorkflowDefinition =
    JsonMapper.instance.readValue(dbData)
}
