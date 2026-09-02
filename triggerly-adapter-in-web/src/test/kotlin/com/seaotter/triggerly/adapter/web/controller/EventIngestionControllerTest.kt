package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.EventRequestDto
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.application.port.EventPublisherPort
import com.seaotter.triggerly.application.port.RateLimiterPort
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.springframework.http.HttpStatus
import kotlin.test.Test
import kotlin.test.assertEquals

class EventIngestionControllerTest {

  private val eventDefinitionPort = mockk<EventDefinitionPort>()
  private val rateLimiterPort = mockk<RateLimiterPort>()
  private val eventPublisherPort = mockk<EventPublisherPort>(relaxed = true)
  private val controller = EventIngestionController(eventDefinitionPort, rateLimiterPort, eventPublisherPort)

  @Test
  fun `등록된 eventCode고 레이트리밋을 통과하면 202를 반환하고 Kafka로 publish한다`() {
    every { rateLimiterPort.tryConsume("t1") } returns true
    every { eventDefinitionPort.findByTenantAndCode("t1", "PURCHASE") } returns EventDefinition("t1", "PURCHASE", "구매")

    val response = controller.ingest(EventRequestDto(tenantId = "t1", eventCode = "PURCHASE"))

    assertEquals(HttpStatus.ACCEPTED, response.statusCode)
    verify { eventPublisherPort.publish(match { it.tenantId == "t1" && it.eventCode == "PURCHASE" }) }
  }

  @Test
  fun `레이트리밋을 초과하면 429를 반환하고 publish하지 않는다`() {
    every { rateLimiterPort.tryConsume("t1") } returns false

    val response = controller.ingest(EventRequestDto(tenantId = "t1", eventCode = "PURCHASE"))

    assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.statusCode)
    verify(exactly = 0) { eventPublisherPort.publish(any()) }
  }

  @Test
  fun `등록되지 않은 eventCode는 400을 반환한다`() {
    every { rateLimiterPort.tryConsume("t1") } returns true
    every { eventDefinitionPort.findByTenantAndCode("t1", "UNKNOWN") } returns null

    val response = controller.ingest(EventRequestDto(tenantId = "t1", eventCode = "UNKNOWN"))

    assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
    verify(exactly = 0) { eventPublisherPort.publish(any()) }
  }
}
