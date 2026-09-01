plugins {
	alias(libs.plugins.kotlin.spring)
	alias(libs.plugins.kotlin.jpa)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.boot.starter.data.elasticsearch)
	implementation(libs.jackson.module.kotlin)
	implementation(libs.jackson.datatype.jsr310)
	runtimeOnly(libs.mysql.connector.j)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.spring.boot.testcontainers)
	testImplementation(libs.testcontainers.junit.jupiter)
	testImplementation(libs.testcontainers.mysql)
	testImplementation(libs.testcontainers.elasticsearch)
	testImplementation(libs.mockk)
}
