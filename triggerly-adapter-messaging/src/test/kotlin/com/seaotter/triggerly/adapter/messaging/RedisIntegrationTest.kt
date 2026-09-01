package com.seaotter.triggerly.adapter.messaging

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

// 여러 테스트 클래스(Task 15가 이 클래스를 상속)가 이 컨테이너를 공유한다(싱글턴 컨테이너 패턴, KafkaIntegrationTest와
// 동일하게 Task 7/11에서 실측 확인된 문제). 브리프 원문은 @Testcontainers/@Container를 사용했으나, 실제로 두 개의
// 테스트 클래스가 이 추상 클래스를 상속해 함께 실행하면(RedisWaitingIndexAdapterTest + 임시 프로브 테스트로 실측)
// 첫 번째 테스트 클래스의 afterAll에서 Testcontainers JUnit5 확장이 공유 static 필드의 컨테이너를 stop()하여,
// 두 번째 테스트 클래스부터 "Connection refused"/소켓 오류가 발생했다. 따라서 @Testcontainers/@Container를 붙이지
// 않고 직접 start()하여 JVM 종료 시 Ryuk가 정리하도록 한다. Redis는 @ServiceConnection 대상이 아니므로
// @DynamicPropertySource로 커넥션 정보를 등록한다.
@SpringBootTest(classes = [MessagingTestApplication::class])
abstract class RedisIntegrationTest {
  companion object {
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
