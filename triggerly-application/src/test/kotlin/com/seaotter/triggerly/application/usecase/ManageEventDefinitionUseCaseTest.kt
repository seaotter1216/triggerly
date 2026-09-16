package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.CacheInvalidationPort
import com.seaotter.triggerly.application.port.CacheInvalidationTopic
import com.seaotter.triggerly.application.port.EventDefinitionPort
import com.seaotter.triggerly.domain.EventDefinition
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class ManageEventDefinitionUseCaseTest {

  private val port = mockk<EventDefinitionPort>()
  private val cacheInvalidationPort = mockk<CacheInvalidationPort>(relaxed = true)
  private val useCase = ManageEventDefinitionUseCase(port, cacheInvalidationPort)

  @Test
  fun `register는 저장 후 EVENT_DEFINITION 캐시 무효화를 발행한다`() {
    every { port.save(any()) } answers { firstArg() }

    useCase.register("t1", "PURCHASE", "구매")

    verify(exactly = 1) { cacheInvalidationPort.publish(CacheInvalidationTopic.EVENT_DEFINITION, "t1:PURCHASE") }
  }
}
