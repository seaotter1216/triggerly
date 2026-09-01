package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer

@SpringBootTest(classes = [PersistenceTestApplication::class])
abstract class MySqlIntegrationTest {
  companion object {
    // 여러 테스트 클래스가 이 컨테이너를 공유한다(싱글턴 컨테이너 패턴). @Container로 관리하면
    // Testcontainers JUnit5 확장이 테스트 클래스마다 afterAll에서 컨테이너를 stop()하여, 두 번째
    // 테스트 클래스부터 "Communications link failure"가 발생한다(실측 확인). 따라서 @Container를
    // 붙이지 않고 직접 start()하여 JVM 종료 시 Ryuk가 정리하도록 한다. @ServiceConnection은
    // @Container 없이도 동작하여 Spring에 커넥션 정보를 그대로 제공한다.
    @ServiceConnection
    @JvmStatic
    val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4").apply {
      withDatabaseName("triggerly")
      start()
    }

    // PersistenceTestApplication은 ElasticsearchIntegrationTest와 공유하는 @SpringBootApplication이라
    // Task 11에서 추가된 com.seaotter.triggerly.adapter.persistence.es 패키지의 ES 어댑터/리포지토리
    // 빈들도 함께 컴포넌트 스캔 대상이 된다. MySQL 컨테이너만 띄우면 ES 커넥션이 없어 컨텍스트 로딩이
    // 실패하므로(실측 확인), 위와 동일한 싱글턴 컨테이너 패턴으로 ES도 함께 띄운다.
    @ServiceConnection
    @JvmStatic
    val elasticsearch: ElasticsearchContainer =
      ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:9.4.2")
        .apply {
          withEnv("xpack.security.enabled", "false")
          start()
        }
  }
}
