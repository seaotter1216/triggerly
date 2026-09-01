plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation("org.springframework:spring-context")
	implementation("org.springframework:spring-tx")
	implementation("org.slf4j:slf4j-api")
	testImplementation(libs.mockk)
	testImplementation(libs.kotest.assertions.core)
}
