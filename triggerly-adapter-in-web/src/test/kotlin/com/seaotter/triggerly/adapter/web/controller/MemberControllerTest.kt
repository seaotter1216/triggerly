package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterMemberRequest
import com.seaotter.triggerly.application.usecase.ManageMemberUseCase
import com.seaotter.triggerly.domain.Member
import io.mockk.every
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class MemberControllerTest {

  private val useCase = mockk<ManageMemberUseCase>()
  private val controller = MemberController(useCase)

  @Test
  fun `register는 유스케이스에 위임한다`() {
    every { useCase.register(any()) } answers { firstArg() }

    val result = controller.register(RegisterMemberRequest("m1", "t1", "ext-1", email = "a@b.com"))

    assertEquals("a@b.com", result.email)
  }

  @Test
  fun `search는 tenantId를 제외한 나머지 파라미터를 조건으로 넘긴다`() {
    every { useCase.search("t1", mapOf("status" to "ACTIVE")) } returns listOf(
      Member(id = "m1", tenantId = "t1", externalMemberId = "ext-1"),
    )

    val result = controller.search("t1", mapOf("tenantId" to "t1", "status" to "ACTIVE"))

    assertEquals(1, result.size)
  }
}
