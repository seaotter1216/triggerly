package com.seaotter.triggerly.sdk

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

data class SdkMemberContext(val externalMemberId: String)

data class SdkEventRequest(
  val tenantId: String,
  val eventCode: String,
  val member: SdkMemberContext? = null,
  val attributes: Map<String, Any?>? = null,
)

class TriggerlyClientException(message: String) : RuntimeException(message)

class TriggerlyClient(
  private val baseUrl: String,
  private val apiKey: String,
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
) {
  private val mapper = jacksonObjectMapper()

  fun sendEvent(
    tenantId: String,
    eventCode: String,
    externalMemberId: String? = null,
    attributes: Map<String, Any?>? = null,
  ) {
    val body = SdkEventRequest(
      tenantId = tenantId,
      eventCode = eventCode,
      member = externalMemberId?.let { SdkMemberContext(it) },
      attributes = attributes,
    )
    val request = HttpRequest.newBuilder()
      .uri(URI.create("$baseUrl/api/v1/events"))
      .header("Content-Type", "application/json")
      .header("Authorization", "Bearer $apiKey")
      .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() !in 200..299) {
      throw TriggerlyClientException("이벤트 전송 실패: status=${response.statusCode()} body=${response.body()}")
    }
  }
}
