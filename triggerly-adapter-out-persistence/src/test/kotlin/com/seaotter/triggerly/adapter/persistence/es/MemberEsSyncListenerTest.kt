package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.ElasticsearchIntegrationTest
import com.seaotter.triggerly.adapter.persistence.MemberSavedEvent
import com.seaotter.triggerly.domain.Member
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import java.util.UUID
import kotlin.test.assertEquals

class MemberEsSyncListenerTest : ElasticsearchIntegrationTest() {

  @Autowired lateinit var eventPublisher: ApplicationEventPublisher
  @Autowired lateinit var repository: MemberElasticsearchRepository
  @Autowired lateinit var operations: ElasticsearchOperations

  @BeforeEach
  fun ensureIndex() {
    val indexOps = operations.indexOps(MemberDocument::class.java)
    if (!indexOps.exists()) indexOps.createWithMapping()
  }

  @Test
  fun `MemberSavedEvent가 발행되면 비동기로 ES에 반영된다`() {
    val tenantId = "t-${UUID.randomUUID()}"
    val member = Member(id = UUID.randomUUID().toString(), tenantId = tenantId, externalMemberId = "ext-1", email = "a@b.com")

    eventPublisher.publishEvent(MemberSavedEvent(member))

    awaitAssert { assertEquals("a@b.com", repository.findById(member.id).orElseThrow().email) }
  }

  private fun <T> awaitAssert(timeoutMs: Long = 3000, intervalMs: Long = 100, block: () -> T): T {
    val deadline = System.currentTimeMillis() + timeoutMs
    var lastError: Throwable? = null
    while (System.currentTimeMillis() < deadline) {
      try {
        return block()
      } catch (e: Throwable) {
        lastError = e
        Thread.sleep(intervalMs)
      }
    }
    throw lastError ?: AssertionError("timeout")
  }
}
