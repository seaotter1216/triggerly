package com.seaotter.triggerly.bootstrap

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@SpringBootTest(
  classes = [TriggerlyClaudeApplication::class],
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
abstract class FullStackIntegrationTest {

  companion object {
    // 이 프로젝트에서 반복적으로 실측 확인된 문제(Task 7/11/13/14): @Container로 관리하면 Testcontainers
    // JUnit5 확장이 테스트 클래스마다 afterAll에서 컨테이너를 stop()한다. 여러 테스트 클래스가 이 추상
    // 클래스를 상속해 컨테이너를 공유할 경우 두 번째 클래스부터 연결 오류가 발생하므로, @Testcontainers/
    // @Container를 붙이지 않고 직접 start()하여 JVM 종료 시 Ryuk가 정리하도록 한다. @ServiceConnection은
    // @Container 없이도 동작하여 Spring에 커넥션 정보를 그대로 제공한다.
    @ServiceConnection
    @JvmStatic
    val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4").apply {
      withDatabaseName("triggerly")
      start()
    }

    // 브리프 원문은 8.15.0이지만, 이 프로젝트가 물고 있는 spring-data-elasticsearch 6.1.0은 Elasticsearch
    // Java 클라이언트 9.4.2를 내장한다. 클라이언트/서버 메이저 버전 불일치로 실패하므로(Task 11에서 실측
    // 확인), 다른 모든 ES 테스트와 동일하게 서버도 9.4.2로 맞춘다.
    @ServiceConnection
    @JvmStatic
    val elasticsearch: ElasticsearchContainer =
      ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:9.4.2")
        .apply {
          withEnv("xpack.security.enabled", "false")
          start()
        }

    @ServiceConnection
    @JvmStatic
    val kafka: KafkaContainer = KafkaContainer("apache/kafka:3.7.0").apply { start() }

    // Redis는 @ServiceConnection 대상이 아니므로(RedisIntegrationTest와 동일한 패턴) @DynamicPropertySource로
    // 커넥션 정보를 등록한다.
    @JvmStatic
    val redis: GenericContainer<*> =
      GenericContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379).apply { start() }

    @JvmStatic
    @DynamicPropertySource
    fun registerRedisProperties(registry: DynamicPropertyRegistry) {
      registry.add("spring.data.redis.host") { redis.host }
      registry.add("spring.data.redis.port") { redis.getMappedPort(6379) }
    }
  }
}
