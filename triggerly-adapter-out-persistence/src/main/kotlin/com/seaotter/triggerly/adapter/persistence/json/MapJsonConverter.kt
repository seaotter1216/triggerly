package com.seaotter.triggerly.adapter.persistence.json

import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter

@Converter
class MapJsonConverter : AttributeConverter<Map<String, Any?>?, String?> {
  override fun convertToDatabaseColumn(attribute: Map<String, Any?>?): String? =
    attribute?.let { JsonMapper.instance.writeValueAsString(it) }

  override fun convertToEntityAttribute(dbData: String?): Map<String, Any?>? =
    dbData?.let { JsonMapper.instance.readValue<Map<String, Any?>>(it) }
}
