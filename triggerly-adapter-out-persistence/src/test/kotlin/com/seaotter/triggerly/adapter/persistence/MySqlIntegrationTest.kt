package com.seaotter.triggerly.adapter.persistence

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.MySQLContainer

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
  }
}
