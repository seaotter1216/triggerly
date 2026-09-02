package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterAttributeDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageAttributeDefinitionUseCase
import com.seaotter.triggerly.domain.AttributeDefinition
import com.seaotter.triggerly.domain.AttributeType
import io.mockk.every
import io.mockk.mockk
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class AttributeDefinitionControllerTest {

  private val useCase = mockk<ManageAttributeDefinitionUseCase>()
  private val controller = AttributeDefinitionController(useCase)

  @Test
  fun `register는 유스케이스에 위임한다`() {
    val expected = AttributeDefinition(UUID.randomUUID().toString(), "t1", null, "amount", "금액", AttributeType.LONG, true)
    every { useCase.register("t1", null, "amount", "금액", AttributeType.LONG, true) } returns expected

    val result = controller.register(RegisterAttributeDefinitionRequest("t1", null, "amount", "금액", AttributeType.LONG, true))

    assertEquals("amount", result.key)
  }
}
