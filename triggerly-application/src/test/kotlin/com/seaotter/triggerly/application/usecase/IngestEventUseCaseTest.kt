package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.engine.WorkflowEngine
import com.seaotter.triggerly.application.port.*
import com.seaotter.triggerly.domain.*
import io.mockk.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import java.time.LocalDateTime

class IngestEventUseCaseTest {

  private val memberCommandPort = mockk<MemberCommandPort>()
  private val eventInstanceRepositoryPort = mockk<EventInstanceRepositoryPort>(relaxed = true)
  private val workflowRepositoryPort = mockk<WorkflowRepositoryPort>()
  private val workflowInstanceRepositoryPort = mockk<WorkflowInstanceRepositoryPort>(relaxed = true)
  private val waitingIndexPort = mockk<WaitingIndexPort>()
  private val workflowEngine = mockk<WorkflowEngine>()

  private val useCase = IngestEventUseCase(
    memberCommandPort,
    eventInstanceRepositoryPort,
    workflowRepositoryPort,
    workflowInstanceRepositoryPort,
    waitingIndexPort,
    workflowEngine,
  )

  @BeforeTest
  fun setUp() {
    every { waitingIndexPort.lookup(any(), any(), any()) } returns emptyList()
  }

  @Test
  fun `신규 externalMemberId는 새 Member를 생성한 뒤 트리거되는 워크플로를 시작한다`() {
    val message = RawEventMessage(
      tenantId = "t1",
      eventCode = "SIGN_UP",
      externalMemberId = "ext-1",
      memberContext = MemberContext(tenantId = "t1", externalMemberId = "ext-1", email = "a@b.com"),
      attributes = null,
      occurredAt = LocalDateTime.now(),
    )
    every { memberCommandPort.findByExternalId("t1", "ext-1") } returns null
    val savedMemberSlot = slot<Member>()
    every { memberCommandPort.save(capture(savedMemberSlot)) } answers { savedMemberSlot.captured }

    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "SIGN_UP") } returns listOf(workflow)
    every { workflowEngine.start(workflow, "t1", any(), any()) } returns mockk()

    useCase.handle(message)

    verify { eventInstanceRepositoryPort.save(match { it.eventCode == "SIGN_UP" && it.tenantId == "t1" }) }
    verify { workflowEngine.start(workflow, "t1", savedMemberSlot.captured.id, any()) }
    assertEquals("a@b.com", savedMemberSlot.captured.email)
  }

  @Test
  fun `대기 인덱스에 매칭되는 인스턴스가 있으면 resumeOnMatch를 호출한다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "REVIEW_ADD", externalMemberId = "ext-1",
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
    )
    val existingMember = Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1")
    every { memberCommandPort.findByExternalId("t1", "ext-1") } returns existingMember
    every { memberCommandPort.save(any()) } returns existingMember
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "REVIEW_ADD") } returns emptyList()
    every { waitingIndexPort.lookup("t1", "REVIEW_ADD", "m1") } returns listOf("instance-1")

    val waitingInstance = mockk<WorkflowInstance> { every { workflowId } returns "wf-1" }
    every { workflowInstanceRepositoryPort.findById("instance-1") } returns waitingInstance
    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findById("t1", "wf-1") } returns workflow
    every { workflowEngine.resumeOnMatch(waitingInstance, workflow, any()) } returns mockk()

    useCase.handle(message)

    verify { workflowEngine.resumeOnMatch(waitingInstance, workflow, any()) }
  }

  @Test
  fun `syntheticTimeoutForInstanceId가 있으면 WAITING 인스턴스만 resumeOnTimeout으로 재개한다`() {
    val message = RawEventMessage(
      tenantId = "t1", eventCode = "__TIMEOUT__", externalMemberId = null,
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      syntheticTimeoutForInstanceId = "instance-2",
    )
    val waitingInstance = mockk<WorkflowInstance> {
      every { status } returns WorkflowInstanceStatus.WAITING
      every { tenantId } returns "t1"
      every { workflowId } returns "wf-1"
    }
    every { workflowInstanceRepositoryPort.findById("instance-2") } returns waitingInstance
    val workflow = mockk<Workflow>()
    every { workflowRepositoryPort.findById("t1", "wf-1") } returns workflow
    every { workflowEngine.resumeOnTimeout(waitingInstance, workflow) } returns mockk()

    useCase.handle(message)

    verify { workflowEngine.resumeOnTimeout(waitingInstance, workflow) }
    verify(exactly = 0) { eventInstanceRepositoryPort.save(any()) }
  }
}
