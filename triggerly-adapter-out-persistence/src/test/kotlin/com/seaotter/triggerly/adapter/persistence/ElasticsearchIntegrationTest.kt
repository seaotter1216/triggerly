package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer

@SpringBootTest(classes = [PersistenceTestApplication::class])
abstract class ElasticsearchIntegrationTest {
  companion object {
    // 여러 테스트 클래스가 이 컨테이너들을 공유한다(싱글턴 컨테이너 패턴). @Container로 관리하면
    // Testcontainers JUnit5 확장이 테스트 클래스마다 afterAll에서 컨테이너를 stop()하여, 두 번째
    // 테스트 클래스부터 접속 실패가 발생한다(Task 7의 MySqlIntegrationTest에서 실측 확인). 따라서
    // @Container를 붙이지 않고 직접 start()하여 JVM 종료 시 Ryuk가 정리하도록 한다. @ServiceConnection은
    // @Container 없이도 동작하여 Spring에 커넥션 정보를 그대로 제공한다.
    @ServiceConnection
    @JvmStatic
    val elasticsearch: ElasticsearchContainer =
      // 브리프 원문은 8.15.0을 사용하지만, 이 프로젝트가 물고 있는 spring-data-elasticsearch 6.1.0은
      // Elasticsearch Java 클라이언트 9.4.2를 내장한다. 클라이언트가 보내는 "compatible-with=9" 헤더를
      // 서버 8.15가 거부해 HEAD 요청(indices.exists)에서 "status 400, Expecting a response body,
      // but none was sent" 예외가 발생함을 실측 확인. 클라이언트 메이저 버전과 맞춘 9.4.2로 조정한다.
      ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:9.4.2")
        .apply {
          withEnv("xpack.security.enabled", "false")
          start()
        }

    // PersistenceTestApplication은 MySqlIntegrationTest와 공유하는 @SpringBootApplication이라
    // com.seaotter.triggerly.adapter.persistence.jpa 패키지의 JPA 리포지토리/어댑터 빈들도 함께
    // 컴포넌트 스캔 대상이 된다. ES 컨테이너만 띄우면 DataSource가 없어 컨텍스트 로딩이 실패하므로
    // (실측 확인), MySqlIntegrationTest와 동일한 싱글턴 컨테이너 패턴으로 MySQL도 함께 띄운다.
    @ServiceConnection
    @JvmStatic
    val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4").apply {
      withDatabaseName("triggerly")
      start()
    }
  }
}
