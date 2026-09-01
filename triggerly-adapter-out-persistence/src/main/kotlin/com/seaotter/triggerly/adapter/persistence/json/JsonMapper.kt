package com.seaotter.triggerly.adapter.persistence.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object JsonMapper {
  val instance: ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
}
