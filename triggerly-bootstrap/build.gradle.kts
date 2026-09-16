plugins {
	alias(libs.plugins.kotlin.spring)
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(project(":triggerly-adapter-in-web"))
	implementation(project(":triggerly-adapter-out-persistence"))
	implementation(project(":triggerly-adapter-messaging"))
	implementation(project(":triggerly-sdk"))

	implementation(libs.spring.boot.starter.actuator)
	implementation(libs.micrometer.registry.prometheus)
	// Spring Boot 4.1의 spring-boot-starter-jackson(→ spring-boot-jackson 모듈)은 신규 Jackson 3
	// (tools.jackson) JsonMapper 빈만 자동 구성하고, 고전 Jackson 2 ObjectMapper 빈은 더 이상
	// 자동 구성하지 않는다(실측 확인: 조건 평가 리포트에서 JacksonAutoConfiguration#jacksonJsonMapper만
	// 매치되고 classic ObjectMapper 빈은 전혀 생성되지 않아, MappingJackson2HttpMessageConverter의
	// @ConditionalOnBean(ObjectMapper)이 실패함). 이 프로젝트의 모든 DTO/도메인 클래스는 classic
	// Jackson2 + jackson-module-kotlin(2.x) 전제로 작성되어 있으므로, 별도 모듈
	// spring-boot-jackson2를 추가해 classic ObjectMapper 자동 구성을 되살린다.
	implementation(libs.spring.boot.jackson2)
	// Spring Boot 4.1은 FlywayAutoConfiguration을 spring-boot-autoconfigure에서 완전히 분리해
	// 별도 모듈 org.springframework.boot:spring-boot-flyway(spring-boot-starter-flyway가 이를
	// 가져온다)로 옮겼다(실측 확인: flyway-core/flyway-mysql만 있을 때는 Flyway 마이그레이션이
	// 전혀 실행되지 않고 조용히 스킵되어, Hibernate ddl-auto=validate가 "missing table" 예외를
	// 던짐 — 로그에 Flyway 관련 라인이 전혀 없었다). spring-boot-starter-flyway를 명시적으로
	// 추가해 autoconfiguration을 되살린다.
	implementation(libs.spring.boot.starter.flyway)
	implementation(libs.flyway.mysql)

	// TriggerlyClaudeApplication이 @EntityScan/@EnableJpaRepositories/@EnableElasticsearchRepositories로
	// triggerly-adapter-out-persistence의 JPA/ES 리포지토리 패키지를 명시적으로 스캔하려면 해당
	// 어노테이션 타입 자체가 이 모듈의 메인 컴파일 클래스패스에 있어야 한다. adapter-out-persistence가
	// 이 스타터들을 implementation(비공개)으로 물고 있어 전이적으로 노출되지 않으므로(실측 확인)
	// 여기서도 명시적으로 추가한다.
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.boot.starter.data.elasticsearch)
	implementation(libs.spring.boot.starter.data.redis)

	// triggerly-adapter-in-web이 spring-boot-starter-web을 implementation(비공개)으로 물고 있어,
	// 이를 implementation으로 의존하는 triggerly-bootstrap의 테스트 소스셋에는 전이적으로 노출되지
	// 않는다(실측 확인: RestTestClient 관련 "Unresolved reference" 컴파일 에러). 풀스택 통합 테스트가
	// 실제 HTTP로 이벤트를 수집 API에 보내므로 테스트 전용으로 명시적으로 추가한다.
	// (spring-boot-starter-data-jpa는 위에서 이미 메인 implementation으로 추가되어 테스트
	// 소스셋에도 전이적으로 노출되므로 별도로 추가할 필요 없다 — WorkflowInstanceJpaRepository 등에 사용.)
	testImplementation(libs.spring.boot.starter.web)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.mysql)
	testImplementation(libs.testcontainers.elasticsearch)
	testImplementation(libs.testcontainers.kafka)
	testImplementation(libs.spring.kafka.test)
	testImplementation(libs.mockk)
}
