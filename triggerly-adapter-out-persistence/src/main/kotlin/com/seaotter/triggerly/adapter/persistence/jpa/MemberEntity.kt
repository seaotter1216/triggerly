package com.seaotter.triggerly.adapter.persistence.jpa

import com.seaotter.triggerly.adapter.persistence.json.MapJsonConverter
import jakarta.persistence.Column
import jakarta.persistence.Convert
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDate
import java.time.LocalDateTime

@Entity
@Table(name = "member")
class MemberEntity(
  @Id var id: String,
  var tenantId: String,
  var externalMemberId: String,
  var name: String?,
  var email: String?,
  var telephone: String?,
  var devicePlatform: String?,
  var gender: String?,
  var birthday: LocalDate?,
  var status: String?,
  var joinedAt: LocalDateTime?,
  var lastLoginAt: LocalDateTime?,
  var withdrawnAt: LocalDateTime?,
  var createdAt: LocalDateTime,
  var marketingSmsAgreed: Boolean,
  var marketingPushAgreed: Boolean,
  var marketingEmailAgreed: Boolean,
  var marketingKakaoAgreed: Boolean,
  var marketingAgreedAt: LocalDateTime?,
  @Column(columnDefinition = "JSON")
  @Convert(converter = MapJsonConverter::class)
  var attributes: Map<String, Any?>?,
)
