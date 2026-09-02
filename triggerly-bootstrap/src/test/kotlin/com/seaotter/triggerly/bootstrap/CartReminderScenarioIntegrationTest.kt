package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.adapter.persistence.jpa.WorkflowInstanceJpaRepository
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.application.usecase.WorkflowInstanceQueryUseCase
import com.seaotter.triggerly.domain.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.MediaType
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.servlet.client.RestTestClient
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.time.DurationUnit

@TestPropertySource(properties = ["triggerly.scheduler.timeout-poll-interval-ms=2000"])
class CartReminderScenarioIntegrationTest : FullStackIntegrationTest() {

  @Autowired lateinit var eventDefinitionUseCase: ManageEventDefinitionUseCase
  @Autowired lateinit var workflowUseCase: ManageWorkflowUseCase
  @Autowired lateinit var workflowInstanceQueryUseCase: WorkflowInstanceQueryUseCase
  @Autowired lateinit var workflowInstanceJpaRepository: WorkflowInstanceJpaRepository

  @LocalServerPort
  var port: Int = 0

  // Spring Boot 4.1(Spring Framework 7)에서 TestRestTemplate이 완전히 제거되었다(실측 확인:
  // spring-boot-test-4.1.0.jar에 org.springframework.boot.test.web.client 패키지 자체가 없고,
  // spring-boot-test-autoconfigure에도 TestRestTemplate을 자동 등록하는 컨텍스트 커스터마이저가
  // 없음). 공식 대체재인 RestTestClient.bindToServer()로 실제 기동된 서버에 HTTP 요청을 보낸다.
  private val restTestClient: RestTestClient by lazy {
    RestTestClient.bindToServer().baseUrl("http://localhost:$port").build()
  }

  private fun cartReminderWorkflow() = WorkflowDefinition(
    trigger = "CART_ADD",
    nodes = listOf(
      Node.Trigger("n1", "CART_ADD"),
      Node.WaitForEvent("n2", WaitEventDefinition("PURCHASE", DurationDto(2, DurationUnit.SECONDS))),
      Node.End("n3"),
      Node.Action("n4", ActionDefinition.IssueCoupon("IT10")),
      Node.End("n5"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Matched),
      Edge("n2", "n4", EdgeRoute.Timeout),
      Edge("n4", "n5", EdgeRoute.Always),
    ),
  )

  private fun seed(tenantId: String): String {
    eventDefinitionUseCase.register(tenantId, "CART_ADD", "장바구니")
    eventDefinitionUseCase.register(tenantId, "PURCHASE", "구매")
    val workflow = Workflow(
      id = UUID.randomUUID().toString(), tenantId = tenantId, triggerEventCode = "CART_ADD",
      definitionJson = cartReminderWorkflow(), status = WorkflowStatus.DRAFT,
      createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
    )
    workflowUseCase.create(workflow)
    workflowUseCase.enable(tenantId, workflow.id)
    return workflow.id
  }

  private fun postEvent(tenantId: String, eventCode: String, externalMemberId: String) {
    val body = mapOf(
      "tenantId" to tenantId, "eventCode" to eventCode,
      "member" to mapOf("externalMemberId" to externalMemberId),
    )
    restTestClient.post().uri("/api/v1/events")
      .contentType(MediaType.APPLICATION_JSON)
      .body(body)
      .exchange()
      .expectStatus().isAccepted()
  }

  private fun awaitCompletedInstance(workflowId: String, timeoutSeconds: Long): String {
    val deadline = System.currentTimeMillis() + timeoutSeconds * 1000
    while (System.currentTimeMillis() < deadline) {
      val completed = workflowInstanceJpaRepository.findAll()
        .firstOrNull { it.workflowId == workflowId && it.status == "COMPLETED" }
      if (completed != null) return completed.id
      Thread.sleep(300)
    }
    error("워크플로 인스턴스가 시간 내에 COMPLETED되지 않음: workflowId=$workflowId")
  }

  @Test
  fun `PURCHASE 이벤트가 타임아웃 전에 오면 Matched 경로로 완료된다`() {
    val tenantId = "it-matched-${UUID.randomUUID()}"
    val workflowId = seed(tenantId)

    postEvent(tenantId, "CART_ADD", "member-matched")
    Thread.sleep(500)
    postEvent(tenantId, "PURCHASE", "member-matched")

    val instanceId = awaitCompletedInstance(workflowId, timeoutSeconds = 10)
    val view = workflowInstanceQueryUseCase.get(instanceId)!!
    assertEquals(WorkflowInstanceStatus.COMPLETED, view.instance.status)
    assertEquals("n3", view.instance.currentNodeId)
  }

  @Test
  fun `PURCHASE 이벤트가 안 오면 스케줄러가 타임아웃시켜 쿠폰 액션으로 완료시킨다`() {
    val tenantId = "it-timeout-${UUID.randomUUID()}"
    val workflowId = seed(tenantId)

    postEvent(tenantId, "CART_ADD", "member-timeout")

    val instanceId = awaitCompletedInstance(workflowId, timeoutSeconds = 20)
    val view = workflowInstanceQueryUseCase.get(instanceId)!!
    assertEquals(WorkflowInstanceStatus.COMPLETED, view.instance.status)
    assertEquals("n5", view.instance.currentNodeId)
    assertEquals(true, view.executions.any { it.nodeId == "n4" && it.result?.contains("IT10") == true })
  }
}
