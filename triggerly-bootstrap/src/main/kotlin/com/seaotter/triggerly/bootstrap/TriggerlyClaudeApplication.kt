package com.seaotter.triggerly.bootstrap

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.persistence.autoconfigure.EntityScan
import org.springframework.boot.runApplication
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.scheduling.annotation.EnableScheduling

// scanBasePackages는 @Component/@Service/@Repository 등 일반 빈에 대한 컴포넌트 스캔 범위만 넓힐 뿐,
// Spring Data JPA/Elasticsearch 리포지토리 스캔은 별도 메커니즘(AutoConfigurationPackages, 이
// @SpringBootApplication이 붙은 클래스 자신의 패키지 = com.seaotter.triggerly.bootstrap 만 기본값으로
// 사용)으로 동작해 이 설정에 영향받지 않는다. 리포지토리들이 sibling 패키지인
// com.seaotter.triggerly.adapter.persistence.jpa / .es 아래에 있어 기본값으로는 전혀 스캔되지
// 않아 "NoSuchBeanDefinitionException: EventInstanceJpaRepository" 등으로 컨텍스트 로딩이
// 실패함을 실측 확인. 명시적으로 basePackages를 지정한다.
@SpringBootApplication(scanBasePackages = ["com.seaotter.triggerly"])
@EnableScheduling
@EntityScan(basePackages = ["com.seaotter.triggerly.adapter.persistence.jpa"])
@EnableJpaRepositories(basePackages = ["com.seaotter.triggerly.adapter.persistence.jpa"])
@EnableElasticsearchRepositories(basePackages = ["com.seaotter.triggerly.adapter.persistence.es"])
class TriggerlyClaudeApplication

fun main(args: Array<String>) {
  runApplication<TriggerlyClaudeApplication>(*args)
}
