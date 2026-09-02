package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventInstanceRepositoryPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test

class EventInstanceCleanupJobTest {

  @Test
  fun `retentionDays 이전 데이터를 삭제한다`() {
    val port = mockk<EventInstanceRepositoryPort>()
    every { port.deleteOlderThan(any()) } returns 5

    EventInstanceCleanupJob(port, retentionDays = 30).cleanup()

    verify { port.deleteOlderThan(any()) }
  }
}
