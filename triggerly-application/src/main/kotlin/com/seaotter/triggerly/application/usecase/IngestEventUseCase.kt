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
    return memberCommandPort.save(
      Member(
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
      ),
    )
  }

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
