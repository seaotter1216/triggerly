package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.engine.WorkflowEngine
import com.seaotter.triggerly.application.port.*
import com.seaotter.triggerly.domain.*
import io.mockk.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
    every { workflowEngine.start(workflow, "t1", any(), any(), any()) } returns mockk()

    useCase.handle(message)

    verify { eventInstanceRepositoryPort.save(match { it.eventCode == "SIGN_UP" && it.tenantId == "t1" }) }
    verify { workflowEngine.start(workflow, "t1", savedMemberSlot.captured.id, any(), message.eventId) }
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

  @Test
  fun `handleBatch는 같은 배치 안의 동일 externalMemberId를 IN절 조회 1회 + saveAll 1회로 묶는다`() {
    val ctx1 = MemberContext(tenantId = "t1", externalMemberId = "ext-1", email = "first@b.com")
    val ctx2 = MemberContext(tenantId = "t1", externalMemberId = "ext-1", telephone = "010-0000-0000")
    val messages = listOf(
      RawEventMessage(
        tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
        memberContext = ctx1, attributes = null, occurredAt = LocalDateTime.now(),
      ),
      RawEventMessage(
        tenantId = "t1", eventCode = "LOGIN", externalMemberId = "ext-1",
        memberContext = ctx2, attributes = null, occurredAt = LocalDateTime.now(),
      ),
    )
    every { memberCommandPort.findByExternalIds("t1", setOf("ext-1")) } returns emptyList()
    val savedMembersSlot = slot<Collection<Member>>()
    every { memberCommandPort.saveAll(capture(savedMembersSlot)) } answers { savedMembersSlot.captured.toList() }
    val savedInstancesSlot = slot<Collection<EventInstance>>()
    every { eventInstanceRepositoryPort.saveAll(capture(savedInstancesSlot)) } answers { savedInstancesSlot.captured.toList() }
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "LOGIN") } returns emptyList()

    useCase.handleBatch(messages)

    verify(exactly = 1) { memberCommandPort.findByExternalIds("t1", setOf("ext-1")) }
    verify(exactly = 1) { memberCommandPort.saveAll(any()) }
    assertEquals(1, savedMembersSlot.captured.size)
    val savedMember = savedMembersSlot.captured.first()
    assertEquals("first@b.com", savedMember.email)
    assertEquals("010-0000-0000", savedMember.telephone)
    assertEquals(2, savedInstancesSlot.captured.size)
  }

  @Test
  fun `handleBatch는 타임아웃 메시지와 일반 이벤트가 섞여도 각각 올바른 경로로 처리한다`() {
    val regular = RawEventMessage(
      tenantId = "t1", eventCode = "SIGN_UP", externalMemberId = null,
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
    )
    val timeout = RawEventMessage(
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
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "SIGN_UP") } returns emptyList()
    val savedInstancesSlot = slot<Collection<EventInstance>>()
    every { eventInstanceRepositoryPort.saveAll(capture(savedInstancesSlot)) } answers { savedInstancesSlot.captured.toList() }

    useCase.handleBatch(listOf(timeout, regular))

    verify { workflowEngine.resumeOnTimeout(waitingInstance, workflow) }
    assertEquals(1, savedInstancesSlot.captured.size)
    assertEquals("SIGN_UP", savedInstancesSlot.captured.first().eventCode)
  }

  @Test
  fun `handleBatch는 같은 tenantId eventCode 조합의 워크플로 정의 조회를 배치 내에서 캐싱한다`() {
    val messages = listOf(
      RawEventMessage(
        tenantId = "t1", eventCode = "CART_ADD", externalMemberId = null,
        memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      ),
      RawEventMessage(
        tenantId = "t1", eventCode = "CART_ADD", externalMemberId = null,
        memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      ),
    )
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "CART_ADD") } returns emptyList()

    useCase.handleBatch(messages)

    verify(exactly = 1) { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "CART_ADD") }
  }

  @Test
  fun `handleBatch는 이미 EventInstance로 저장된 eventId(재배달)를 만나면 엔진 실행 없이 스킵한다`() {
    val redelivered = RawEventMessage(
      tenantId = "t1", eventCode = "PURCHASE", externalMemberId = null,
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(),
      eventId = "evt-already-done",
    )
    every { eventInstanceRepositoryPort.findExistingIds(listOf("evt-already-done")) } returns setOf("evt-already-done")

    useCase.handleBatch(listOf(redelivered))

    verify(exactly = 0) { workflowRepositoryPort.findEnabledByTriggerEventCode(any(), any()) }
    verify(exactly = 0) { eventInstanceRepositoryPort.saveAll(any()) }
  }

  @Test
  fun `handleBatch는 배치 중간 레코드가 실패하면 그 인덱스를 담은 예외를 던지고, 그 전까지 성공한 건 먼저 저장한다`() {
    val ok = RawEventMessage(
      tenantId = "t1", eventCode = "LOGIN", externalMemberId = null,
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(), eventId = "evt-ok",
    )
    val bad = RawEventMessage(
      tenantId = "t1", eventCode = "SIGN_UP", externalMemberId = null,
      memberContext = null, attributes = null, occurredAt = LocalDateTime.now(), eventId = "evt-bad",
    )
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "LOGIN") } returns emptyList()
    every { workflowRepositoryPort.findEnabledByTriggerEventCode("t1", "SIGN_UP") } throws RuntimeException("boom")
    val savedSlot = slot<Collection<EventInstance>>()
    every { eventInstanceRepositoryPort.saveAll(capture(savedSlot)) } answers { savedSlot.captured.toList() }

    val ex = assertFailsWith<BatchEventProcessingException> { useCase.handleBatch(listOf(ok, bad)) }

    assertEquals(1, ex.failedIndex)
    assertEquals("evt-bad", ex.eventId)
    assertEquals(1, savedSlot.captured.size)
    assertEquals("evt-ok", savedSlot.captured.first().id)
  }
}
