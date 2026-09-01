package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.MemberSavedEvent
import com.seaotter.triggerly.domain.Member
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class MemberEsSyncListener(private val repository: MemberElasticsearchRepository) {

  @Async("memberSyncExecutor")
  @EventListener
  fun onMemberSaved(event: MemberSavedEvent) {
    repository.save(event.member.toDocument())
  }

  private fun Member.toDocument() = MemberDocument(
    id = id, tenantId = tenantId, externalMemberId = externalMemberId, name = name, email = email,
    telephone = telephone, devicePlatform = devicePlatform?.name, gender = gender?.name, birthday = birthday,
    status = status?.name, attributes = attributes,
  )
}
