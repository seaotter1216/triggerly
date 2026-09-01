package com.seaotter.triggerly.adapter.persistence.es

import com.seaotter.triggerly.adapter.persistence.ElasticsearchIntegrationTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchOperations
import java.util.UUID
import kotlin.test.assertEquals

class MemberSearchAdapterTest : ElasticsearchIntegrationTest() {

  @Autowired lateinit var adapter: MemberSearchAdapter
  @Autowired lateinit var repository: MemberElasticsearchRepository
  @Autowired lateinit var operations: ElasticsearchOperations

  @BeforeEach
  fun setUp() {
    val indexOps = operations.indexOps(MemberDocument::class.java)
    if (!indexOps.exists()) indexOps.createWithMapping()
  }

  @Test
  fun `search는 tenantId와 추가 조건을 AND로 결합해 검색한다`() {
    val tenantId = "t-${UUID.randomUUID()}"
    repository.save(MemberDocument(UUID.randomUUID().toString(), tenantId, "ext-1", null, null, null, null, null, null, "ACTIVE", null))
    repository.save(MemberDocument(UUID.randomUUID().toString(), tenantId, "ext-2", null, null, null, null, null, null, "WITHDRAWN", null))
    operations.indexOps(MemberDocument::class.java).refresh()

    val result = adapter.search(tenantId, mapOf("status" to "ACTIVE"))

    assertEquals(1, result.size)
    assertEquals("ext-1", result[0].externalMemberId)
  }
}
