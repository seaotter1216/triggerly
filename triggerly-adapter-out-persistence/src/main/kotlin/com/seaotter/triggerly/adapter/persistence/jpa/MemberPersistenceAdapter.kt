package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.MemberSavedEvent
import com.seaotter.triggerly.application.port.MemberCommandPort
import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.Member
import com.seaotter.triggerly.domain.MemberStatus
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
class MemberPersistenceAdapter(
  private val repository: MemberJpaRepository,
  private val eventPublisher: ApplicationEventPublisher,
) : MemberCommandPort {

  override fun save(member: Member): Member {
    val saved = repository.save(member.toEntity()).toDomain()
    eventPublisher.publishEvent(MemberSavedEvent(saved))
    return saved
  }

  override fun findById(id: String): Member? = repository.findById(id).orElse(null)?.toDomain()

  override fun findByExternalId(tenantId: String, externalMemberId: String): Member? =
    repository.findByTenantIdAndExternalMemberId(tenantId, externalMemberId)?.toDomain()

  private fun Member.toEntity() = MemberEntity(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.name, gender = gender?.name, birthday = birthday,
    status = status?.name, joinedAt = joinedAt, lastLoginAt = lastLoginAt, withdrawnAt = withdrawnAt,
    createdAt = createdAt, marketingSmsAgreed = marketingSmsAgreed, marketingPushAgreed = marketingPushAgreed,
    marketingEmailAgreed = marketingEmailAgreed, marketingKakaoAgreed = marketingKakaoAgreed,
    marketingAgreedAt = marketingAgreedAt, attributes = attributes,
  )

  private fun MemberEntity.toDomain() = Member(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.let { DevicePlatform.valueOf(it) },
    gender = gender?.let { Gender.valueOf(it) }, birthday = birthday, status = status?.let { MemberStatus.valueOf(it) },
    joinedAt = joinedAt, lastLoginAt = lastLoginAt, withdrawnAt = withdrawnAt, createdAt = createdAt,
    marketingSmsAgreed = marketingSmsAgreed, marketingPushAgreed = marketingPushAgreed,
    marketingEmailAgreed = marketingEmailAgreed, marketingKakaoAgreed = marketingKakaoAgreed,
    marketingAgreedAt = marketingAgreedAt, attributes = attributes,
  )
}
