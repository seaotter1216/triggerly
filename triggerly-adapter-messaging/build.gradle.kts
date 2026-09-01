plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(libs.spring.boot.starter.kafka)
	implementation(libs.spring.boot.starter.data.redis)
	implementation(libs.jackson.module.kotlin)
	implementation(libs.jackson.datatype.jsr310)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.kafka)
	testImplementation(libs.spring.kafka.test)
	testImplementation(libs.mockk)
}
