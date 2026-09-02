package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.AttributeType

data class RegisterAttributeDefinitionRequest(
  val tenantId: String,
  val eventDefinitionId: String?,
  val key: String,
  val displayName: String,
  val type: AttributeType,
  val filterable: Boolean = false,
)
