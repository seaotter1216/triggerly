package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.WorkflowEngine
import com.seaotter.triggerly.application.port.*
import com.seaotter.triggerly.domain.*
import org.slf4j.LoggerFactory
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

  private val log = LoggerFactory.getLogger(IngestEventUseCase::class.java)

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
      .forEach { workflow -> workflowEngine.start(workflow, message.tenantId, member?.id, context, message.eventId) }

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
  // 멤버 조회/저장을 (tenantId) 단위로 묶어 DB 왕복 횟수를 줄인다.
  // 워크플로 상태 전이(start/resumeOnMatch)는 이벤트마다 결과가 달라 배치화할 수 없으므로 그대로 순회한다.
  //
  // [재시도/멱등 처리] 같은 배치가 실패해서 카프카가 다시 배달하더라도(레코드는 그대로, RawEventMessage.eventId도
  // 그대로) 이미 액션(쿠폰 발급 등)까지 실행된 이벤트를 또 실행하면 안 된다. 그래서:
  //   1) 배치 시작 시 eventId가 이미 EventInstance에 저장돼 있는 건("이전 시도에서 엔진 실행까지 끝난 이벤트")
  //      찾아서 통째로 스킵한다.
  //   2) 이벤트 하나의 엔진 실행이 "성공적으로 끝난 뒤에만" EventInstance를 저장 대상에 담는다 — 실행 도중
  //      실패하면 이 이벤트는 저장되지 않으므로, 재시도 때 다시 (1)에 안 걸리고 처음부터 재실행된다.
  //   3) 레코드 하나가 실패하면 그 인덱스까지는 이미 성공했으니 먼저 커밋(saveAll)해두고,
  //      실패한 인덱스를 BatchEventProcessingException으로 알려서 그 레코드부터만 재시도/DLT 대상이 되게 한다
  //      (WorkflowTriggerConsumer가 이걸 BatchListenerFailedException으로 바꿔 카프카 컨테이너에 전달한다).
  //   4) 한 이벤트가 워크플로 여러 개를 트리거하는데 그중 하나가 실패해도, 앞서 이미 끝낸 워크플로들은
  //      재시도 때 다시 실행되지 않는다 - WorkflowEngine.start()가 인스턴스 id를 eventId+workflowId로 고정해
  //      이미 있는 인스턴스를 그대로 반환하기 때문(WorkflowEngine.kt 참고).
  fun handleBatch(messages: List<RawEventMessage>) {
    val regularMessages = messages.filter { it.syntheticTimeoutForInstanceId == null }
    val alreadyProcessedEventIds = eventInstanceRepositoryPort.findExistingIds(regularMessages.map { it.eventId })
    val pendingMessages = regularMessages.filterNot { it.eventId in alreadyProcessedEventIds }
    val resolvedMembers = bulkResolveMembers(pendingMessages)

    val workflowCache = mutableMapOf<Pair<String, String>, List<Workflow>>()
    val processedInstances = mutableListOf<EventInstance>()

    messages.forEachIndexed { index, message ->
      try {
        if (message.syntheticTimeoutForInstanceId != null) {
          // 타임아웃 이벤트는 handleTimeout 안의 "인스턴스 상태가 WAITING일 때만" 가드로 이미 멱등하다
          // (같은 인스턴스가 이미 처리됐으면 상태가 바뀌어 있어 자연히 재실행되지 않음). eventId 판별 불필요.
          handleTimeout(message.syntheticTimeoutForInstanceId)
          return@forEachIndexed
        }
        if (message.eventId in alreadyProcessedEventIds) {
          log.debug("이미 처리된 이벤트 재배달 - 스킵: eventId={}", message.eventId)
          return@forEachIndexed
        }

        val member = message.externalMemberId?.let { resolvedMembers[message.tenantId to it] }
        val context = buildContext(message, member)

        workflowCache.getOrPut(message.tenantId to message.eventCode) {
          workflowRepositoryPort.findEnabledByTriggerEventCode(message.tenantId, message.eventCode)
        }.forEach { workflow -> workflowEngine.start(workflow, message.tenantId, member?.id, context, message.eventId) }

        if (member != null) {
          waitingIndexPort.lookup(message.tenantId, message.eventCode, member.id)
            .mapNotNull { workflowInstanceRepositoryPort.findById(it) }
            .forEach { waiting ->
              val workflow = workflowRepositoryPort.findById(message.tenantId, waiting.workflowId) ?: return@forEach
              workflowEngine.resumeOnMatch(waiting, workflow, context)
            }
        }

        processedInstances += EventInstance(
          id = message.eventId,
          tenantId = message.tenantId,
          eventCode = message.eventCode,
          occurredAt = message.occurredAt,
          memberId = member?.id,
          attributes = message.attributes,
        )
      } catch (ex: Exception) {
        if (processedInstances.isNotEmpty()) eventInstanceRepositoryPort.saveAll(processedInstances)
        throw BatchEventProcessingException(index, message.eventId, ex)
      }
    }

    if (processedInstances.isNotEmpty()) eventInstanceRepositoryPort.saveAll(processedInstances)
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
