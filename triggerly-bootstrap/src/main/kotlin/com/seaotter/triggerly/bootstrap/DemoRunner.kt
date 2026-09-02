package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.usecase.ManageAttributeDefinitionUseCase
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.application.usecase.ManageMemberUseCase
import com.seaotter.triggerly.application.usecase.ManageWorkflowUseCase
import com.seaotter.triggerly.domain.AttributeType
import com.seaotter.triggerly.domain.Member
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowStatus
import com.seaotter.triggerly.sdk.TriggerlyClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

@Component
@Profile("demo")
class DemoRunner(
  private val eventDefinitionUseCase: ManageEventDefinitionUseCase,
  private val attributeDefinitionUseCase: ManageAttributeDefinitionUseCase,
  private val workflowUseCase: ManageWorkflowUseCase,
  private val memberUseCase: ManageMemberUseCase,
  @Value("\${server.port:8080}") private val port: Int,
) : CommandLineRunner {

  private val log = LoggerFactory.getLogger(DemoRunner::class.java)
  private val tenantId = "demo-tenant"

  override fun run(vararg args: String) {
    log.info("=== triggerly-claude 데모 시작 ===")
    seedDefinitions()
    seedMembers()
    seedWorkflows()

    val client = TriggerlyClient(baseUrl = "http://localhost:$port", apiKey = "demo-key")

    log.info("--- 시나리오 1: 장바구니 리마인드 (1분 안에 구매 안 하면 쿠폰) ---")
    client.sendEvent(tenantId, "CART_ADD", "member-1", mapOf("productId" to "P1"))

    log.info("--- 시나리오 2: 생일 쿠폰 (오늘 로그인) ---")
    client.sendEvent(tenantId, "LOGIN", "member-2")

    log.info("--- 시나리오 3: 가입 환영 알림 (30초 뒤 알림톡) ---")
    client.sendEvent(tenantId, "SIGN_UP", "member-3")

    log.info("--- 시나리오 4: 리뷰 유도 (5분 안에 리뷰 없으면 최근 30일 리뷰수 확인) ---")
    client.sendEvent(tenantId, "PURCHASE", "member-4", mapOf("amount" to 30000))

    log.info("=== 시드 완료. 콘솔에서 [DEMO ACTION] 로그를 지켜보세요 (최대 5분 소요) ===")
  }

  private fun seedDefinitions() {
    listOf(
      "CART_ADD" to "장바구니 담기", "PURCHASE" to "구매", "REVIEW_ADD" to "리뷰 작성",
      "LOGIN" to "로그인", "SIGN_UP" to "회원가입",
    ).forEach { (code, name) -> eventDefinitionUseCase.register(tenantId, code, name) }
    attributeDefinitionUseCase.register(tenantId, null, "amount", "결제 금액", AttributeType.LONG, true)
  }

  private fun seedMembers() {
    memberUseCase.register(Member(id = "member-1", tenantId = tenantId, externalMemberId = "member-1"))
    memberUseCase.register(
      Member(id = "member-2", tenantId = tenantId, externalMemberId = "member-2", birthday = LocalDate.now()),
    )
    memberUseCase.register(Member(id = "member-3", tenantId = tenantId, externalMemberId = "member-3"))
    memberUseCase.register(Member(id = "member-4", tenantId = tenantId, externalMemberId = "member-4"))
  }

  private fun seedWorkflows() {
    listOf(
      Triple("장바구니 리마인드", "CART_ADD", DemoWorkflows.cartReminder()),
      Triple("생일 쿠폰", "LOGIN", DemoWorkflows.birthdayCoupon()),
      Triple("가입 환영 알림", "SIGN_UP", DemoWorkflows.welcomeDelay()),
      Triple("리뷰 유도", "PURCHASE", DemoWorkflows.reviewNudge()),
    ).forEach { (name, triggerEventCode, definition) ->
      val workflow = Workflow(
        id = UUID.randomUUID().toString(), tenantId = tenantId, name = name, triggerEventCode = triggerEventCode,
        definitionJson = definition, status = WorkflowStatus.DRAFT,
        createdAt = LocalDateTime.now(), lastUpdatedAt = LocalDateTime.now(),
      )
      workflowUseCase.create(workflow)
      workflowUseCase.enable(tenantId, workflow.id)
    }
  }
}
