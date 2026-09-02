package com.seaotter.triggerly.adapter.web.dto

import com.seaotter.triggerly.domain.DevicePlatform
import com.seaotter.triggerly.domain.Gender
import com.seaotter.triggerly.domain.Member
import java.time.LocalDate

data class RegisterMemberRequest(
  val id: String,
  val tenantId: String,
  val externalMemberId: String,
  val name: String? = null,
  val email: String? = null,
  val telephone: String? = null,
  val devicePlatform: DevicePlatform? = null,
  val gender: Gender? = null,
  val birthday: LocalDate? = null,
  val attributes: Map<String, Any?>? = null,
) {
  fun toDomain() = Member(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform, gender = gender, birthday = birthday,
    attributes = attributes,
  )
}
