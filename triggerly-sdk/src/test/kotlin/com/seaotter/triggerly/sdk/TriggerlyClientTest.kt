package com.seaotter.triggerly.sdk

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TriggerlyClientTest {

  private lateinit var server: HttpServer
  private var lastRequestBody: String = ""
  private var lastPath: String = ""
  private var responseStatus = 202

  @BeforeTest
  fun startServer() {
    responseStatus = 202
    server = HttpServer.create(InetSocketAddress(0), 0)
    server.createContext("/api/v1/events") { exchange ->
      lastPath = exchange.requestURI.path
      lastRequestBody = exchange.requestBody.readBytes().decodeToString()
      exchange.sendResponseHeaders(responseStatus, -1)
      exchange.close()
    }
    server.start()
  }

  @AfterTest
  fun stopServer() {
    server.stop(0)
  }

  @Test
  fun `sendEvent는 POST api v1 events로 JSON 바디를 전송한다`() {
    val client = TriggerlyClient(baseUrl = "http://localhost:${server.address.port}", apiKey = "key")

    client.sendEvent("t1", "PURCHASE", "ext-1", mapOf("amount" to 1000))

    assertEquals("/api/v1/events", lastPath)
    assertTrue(lastRequestBody.contains("PURCHASE"))
    assertTrue(lastRequestBody.contains("ext-1"))
  }

  @Test
  fun `2xx가 아닌 응답이면 TriggerlyClientException을 던진다`() {
    responseStatus = 500
    val client = TriggerlyClient(baseUrl = "http://localhost:${server.address.port}", apiKey = "key")

    assertFailsWith<TriggerlyClientException> { client.sendEvent("t1", "PURCHASE") }
  }
}
