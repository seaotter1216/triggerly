package com.seaotter.triggerly.domain

import java.time.LocalDate
import java.time.LocalDateTime

class Member(
  val id: String,
  val tenantId: String,
  val externalMemberId: String,
  val name: String? = null,
  var email: String? = null,
  var telephone: String? = null,
  var devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  var birthday: LocalDate? = null,

  var status: MemberStatus? = null,

  val joinedAt: LocalDateTime? = null,
  var lastLoginAt: LocalDateTime? = null,
  var withdrawnAt: LocalDateTime? = null,
  val createdAt: LocalDateTime = LocalDateTime.now(),

  var marketingSmsAgreed: Boolean = false,
  var marketingPushAgreed: Boolean = false,
  var marketingEmailAgreed: Boolean = false,
  var marketingKakaoAgreed: Boolean = false,
  var marketingAgreedAt: LocalDateTime? = null,

  var attributes: Map<String, Any?>? = null,
)

enum class MemberStatus { ACTIVE, WITHDRAWN, DORMANT, BLOCKED }
enum class Gender { MALE, FEMALE }
enum class DevicePlatform { ANDROID, IOS, WEB }

// EventRequest에 실려오는 회원 컨텍스트 (SDK 호출부가 채워서 보냄)
class MemberContext(
  val tenantId: String,
  val externalMemberId: String,
  val name: String? = null,
  var email: String? = null,
  var telephone: String? = null,
  var devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  var birthday: LocalDate? = null,
  var status: MemberStatus? = null,
  val joinedAt: LocalDateTime? = null,
  var lastLoginAt: LocalDateTime? = null,
  var withdrawnAt: LocalDateTime? = null,
  val createdAt: LocalDateTime = LocalDateTime.now(),
  var marketingSmsAgreed: Boolean = false,
  var marketingPushAgreed: Boolean = false,
  var marketingEmailAgreed: Boolean = false,
  var marketingKakaoAgreed: Boolean = false,
  var marketingAgreedAt: LocalDateTime? = null,
)
