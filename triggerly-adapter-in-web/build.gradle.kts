plugins {
	alias(libs.plugins.kotlin.spring)
}

dependencies {
	implementation(project(":triggerly-domain"))
	implementation(project(":triggerly-application"))
	implementation(libs.spring.boot.starter.web)
	implementation(libs.springdoc.openapi.webmvc.ui)
	implementation(libs.jackson.module.kotlin)

	testImplementation(libs.spring.boot.starter.test)
	testImplementation(libs.mockk)
}
