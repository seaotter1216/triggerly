package com.seaotter.triggerly.adapter.web.controller

import com.seaotter.triggerly.adapter.web.dto.RegisterEventDefinitionRequest
import com.seaotter.triggerly.application.usecase.ManageEventDefinitionUseCase
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class EventDefinitionControllerTest {

  private val useCase = mockk<ManageEventDefinitionUseCase>()
  private val controller = EventDefinitionController(useCase)

  @Test
  fun `register는 유스케이스에 위임한다`() {
    every { useCase.register("t1", "PURCHASE", "구매") } returns EventDefinition("t1", "PURCHASE", "구매")

    val result = controller.register(RegisterEventDefinitionRequest("t1", "PURCHASE", "구매"))

    assertEquals("PURCHASE", result.code)
  }

  @Test
  fun `list는 테넌트의 전체 목록을 반환한다`() {
    every { useCase.list("t1") } returns listOf(EventDefinition("t1", "PURCHASE", "구매"))

    assertEquals(1, controller.list("t1").size)
  }
}
