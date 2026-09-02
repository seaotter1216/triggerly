package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterMemberRequest
import com.seaotter.triggerly.application.usecase.ManageMemberUseCase
import com.seaotter.triggerly.domain.Member
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/members")
class MemberController(private val useCase: ManageMemberUseCase) {

  @PostMapping
  fun register(@RequestBody request: RegisterMemberRequest): Member = useCase.register(request.toDomain())

  @GetMapping("/search")
  fun search(@RequestParam tenantId: String, @RequestParam allParams: Map<String, String>): List<Member> =
    useCase.search(tenantId, allParams - "tenantId")
}
