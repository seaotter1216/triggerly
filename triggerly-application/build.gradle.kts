plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation("org.springframework:spring-context")
	implementation("org.springframework:spring-tx")
	testImplementation(libs.mockk)
	testImplementation(libs.kotest.assertions.core)
}
