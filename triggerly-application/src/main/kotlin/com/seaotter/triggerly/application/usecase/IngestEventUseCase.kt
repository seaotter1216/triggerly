package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.WorkflowEngine
import com.seaotter.triggerly.application.port.*
import com.seaotter.triggerly.domain.*
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

@Service
class IngestEventUseCase(
  private val memberCommandPort: MemberCommandPort,
  private val eventInstanceRepositoryPort: EventInstanceRepositoryPort,
  private val workflowRepositoryPort: WorkflowRepositoryPort,
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val waitingIndexPort: WaitingIndexPort,
  private val workflowEngine: WorkflowEngine,
) {

  fun handle(message: RawEventMessage) {
    if (message.syntheticTimeoutForInstanceId != null) {
      handleTimeout(message.syntheticTimeoutForInstanceId)
      return
    }

    val member = resolveOrCreateMember(message.tenantId, message.externalMemberId, message.memberContext)

    eventInstanceRepositoryPort.save(
      EventInstance(
        id = UUID.randomUUID().toString(),
        tenantId = message.tenantId,
        eventCode = message.eventCode,
        occurredAt = message.occurredAt,
        memberId = member?.id,
        attributes = message.attributes,
      ),
    )

    val context = buildContext(message, member)

    workflowRepositoryPort.findEnabledByTriggerEventCode(message.tenantId, message.eventCode)
      .forEach { workflow -> workflowEngine.start(workflow, message.tenantId, member?.id, context) }

    if (member != null) {
      waitingIndexPort.lookup(message.tenantId, message.eventCode, member.id)
        .mapNotNull { workflowInstanceRepositoryPort.findById(it) }
        .forEach { waiting ->
          val workflow = workflowRepositoryPort.findById(message.tenantId, waiting.workflowId) ?: return@forEach
          workflowEngine.resumeOnMatch(waiting, workflow, context)
        }
    }
  }

  // Kafka 컨슈머(WorkflowTriggerConsumer)가 한 번의 poll로 끌어온 배치를 통째로 넘기는 진입점.
  // 트래픽이 적을 때는 배치 크기가 1~2건이라 handle()과 사실상 동일하게 동작하고, 트래픽이 몰릴 때는
  // 멤버 조회/저장과 EventInstance 저장을 (tenantId, eventCode) 단위로 묶어 DB 왕복 횟수를 줄인다.
  // 워크플로 상태 전이(start/resumeOnMatch)는 이벤트마다 결과가 달라 배치화할 수 없으므로 그대로 순회한다.
  fun handleBatch(messages: List<RawEventMessage>) {
    val (timeoutMessages, regularMessages) = messages.partition { it.syntheticTimeoutForInstanceId != null }
    timeoutMessages.forEach { handleTimeout(it.syntheticTimeoutForInstanceId!!) }
    if (regularMessages.isEmpty()) return

    val resolvedMembers = bulkResolveMembers(regularMessages)

    eventInstanceRepositoryPort.saveAll(
      regularMessages.map { message ->
        val member = message.externalMemberId?.let { resolvedMembers[message.tenantId to it] }
        EventInstance(
          id = UUID.randomUUID().toString(),
          tenantId = message.tenantId,
          eventCode = message.eventCode,
          occurredAt = message.occurredAt,
          memberId = member?.id,
          attributes = message.attributes,
        )
      },
    )

    val workflowCache = mutableMapOf<Pair<String, String>, List<Workflow>>()
    regularMessages.forEach { message ->
      val member = message.externalMemberId?.let { resolvedMembers[message.tenantId to it] }
      val context = buildContext(message, member)

      workflowCache.getOrPut(message.tenantId to message.eventCode) {
        workflowRepositoryPort.findEnabledByTriggerEventCode(message.tenantId, message.eventCode)
      }.forEach { workflow -> workflowEngine.start(workflow, message.tenantId, member?.id, context) }

      if (member != null) {
        waitingIndexPort.lookup(message.tenantId, message.eventCode, member.id)
          .mapNotNull { workflowInstanceRepositoryPort.findById(it) }
          .forEach { waiting ->
            val workflow = workflowRepositoryPort.findById(message.tenantId, waiting.workflowId) ?: return@forEach
            workflowEngine.resumeOnMatch(waiting, workflow, context)
          }
      }
    }
  }

  // tenantId 단위로 findByExternalIds(IN절) 1회 + saveAll 1회로 멤버를 일괄 조회/갱신/생성한다.
  // 같은 배치 안에 동일한 (tenantId, externalMemberId)가 여러 번 오면 컨텍스트를 순서대로 누적 적용하고
  // 마지막 상태 1건만 저장한다.
  private fun bulkResolveMembers(messages: List<RawEventMessage>): Map<Pair<String, String>, Member> {
    val existingByKey = mutableMapOf<Pair<String, String>, Member>()
    messages.filter { it.externalMemberId != null }.groupBy { it.tenantId }.forEach { (tenantId, msgs) ->
      val externalIds = msgs.mapNotNull { it.externalMemberId }.toSet()
      memberCommandPort.findByExternalIds(tenantId, externalIds)
        .forEach { existingByKey[tenantId to it.externalMemberId] = it }
    }

    val toSave = LinkedHashMap<Pair<String, String>, Member>()
    messages.forEach { message ->
      val externalId = message.externalMemberId ?: return@forEach
      val key = message.tenantId to externalId
      val member = toSave[key] ?: existingByKey[key]
      toSave[key] = if (member != null) {
        message.memberContext?.let { applyContext(member, it) }
        member
      } else {
        newMember(message.tenantId, externalId, message.memberContext)
      }
    }
    if (toSave.isEmpty()) return emptyMap()

    return memberCommandPort.saveAll(toSave.values).associateBy { it.tenantId to it.externalMemberId }
  }

  private fun handleTimeout(instanceId: String) {
    val instance = workflowInstanceRepositoryPort.findById(instanceId) ?: return
    if (instance.status != WorkflowInstanceStatus.WAITING) return
    val workflow = workflowRepositoryPort.findById(instance.tenantId, instance.workflowId) ?: return
    workflowEngine.resumeOnTimeout(instance, workflow)
  }

  private fun resolveOrCreateMember(tenantId: String, externalMemberId: String?, context: MemberContext?): Member? {
    if (externalMemberId == null) return null
    val existing = memberCommandPort.findByExternalId(tenantId, externalMemberId)
    if (existing != null) {
      context?.let { applyContext(existing, it) }
      return memberCommandPort.save(existing)
    }
    return memberCommandPort.save(newMember(tenantId, externalMemberId, context))
  }

  private fun newMember(tenantId: String, externalMemberId: String, context: MemberContext?) = Member(
    id = UUID.randomUUID().toString(),
    tenantId = tenantId,
    externalMemberId = externalMemberId,
    name = context?.name,
    email = context?.email,
    telephone = context?.telephone,
    devicePlatform = context?.devicePlatform,
    gender = context?.gender,
    birthday = context?.birthday,
    status = context?.status ?: MemberStatus.ACTIVE,
    joinedAt = context?.joinedAt ?: LocalDateTime.now(),
    lastLoginAt = context?.lastLoginAt,
    marketingSmsAgreed = context?.marketingSmsAgreed ?: false,
    marketingPushAgreed = context?.marketingPushAgreed ?: false,
    marketingEmailAgreed = context?.marketingEmailAgreed ?: false,
    marketingKakaoAgreed = context?.marketingKakaoAgreed ?: false,
    marketingAgreedAt = context?.marketingAgreedAt,
  )

  private fun applyContext(member: Member, context: MemberContext) {
    context.email?.let { member.email = it }
    context.telephone?.let { member.telephone = it }
    context.devicePlatform?.let { member.devicePlatform = it }
    context.birthday?.let { member.birthday = it }
    context.status?.let { member.status = it }
    context.lastLoginAt?.let { member.lastLoginAt = it }
  }

  private fun buildContext(message: RawEventMessage, member: Member?): Map<String, Any?> {
    val ctx = mutableMapOf<String, Any?>(
      "event.eventCode" to message.eventCode,
      "event.occurredAt" to message.occurredAt,
    )
    message.attributes?.forEach { (k, v) -> ctx["event.$k"] = v }
    if (member != null) {
      ctx["member.id"] = member.id
      ctx["member.email"] = member.email
      ctx["member.telephone"] = member.telephone
      ctx["member.devicePlatform"] = member.devicePlatform?.name
      ctx["member.gender"] = member.gender?.name
      ctx["member.birthday"] = member.birthday
      ctx["member.status"] = member.status?.name
      val today = LocalDate.now()
      ctx["member.isBirthdayToday"] = member.birthday?.let { it.monthValue == today.monthValue && it.dayOfMonth == today.dayOfMonth } ?: false
      member.attributes?.forEach { (k, v) -> ctx["member.attributes.$k"] = v }
    }
    return ctx
  }
}
