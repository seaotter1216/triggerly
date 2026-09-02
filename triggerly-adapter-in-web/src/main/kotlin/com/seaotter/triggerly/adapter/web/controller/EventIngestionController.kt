package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.EventRequestDto
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RateLimiterPort
import com.seaotter.triggerly.application.port.RawEventMessage
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.time.LocalDateTime

@RestController
class EventIngestionController(
  private val eventDefinitionPort: EventDefinitionPort,
  private val rateLimiterPort: RateLimiterPort,
  private val eventPublisherPort: EventPublisherPort,
) {

  @PostMapping("/api/v1/events")
  fun ingest(@RequestBody request: EventRequestDto): ResponseEntity<Any> {
    if (!rateLimiterPort.tryConsume(request.tenantId)) {
      return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).header(HttpHeaders.RETRY_AFTER, "1").build()
    }
    if (eventDefinitionPort.findByTenantAndCode(request.tenantId, request.eventCode) == null) {
      return ResponseEntity.badRequest().body(mapOf("error" to "unknown eventCode: ${request.eventCode}"))
    }

    eventPublisherPort.publish(
      RawEventMessage(
        tenantId = request.tenantId,
        eventCode = request.eventCode,
        externalMemberId = request.member?.externalMemberId,
        memberContext = request.member?.toDomain(request.tenantId),
        attributes = request.attributes,
        occurredAt = LocalDateTime.now(),
      ),
    )
    return ResponseEntity.accepted().build()
  }
}
