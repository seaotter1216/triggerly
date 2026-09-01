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
	implementation(libs.flyway.core)
	implementation(libs.flyway.mysql)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.mysql)
	testImplementation(libs.testcontainers.elasticsearch)
	testImplementation(libs.testcontainers.kafka)
	testImplementation(libs.spring.kafka.test)
	testImplementation(libs.mockk)
}

tasks.named<Jar>("jar") {
	enabled = false
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
	enabled = false
}
