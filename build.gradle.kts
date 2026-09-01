plugins {
	alias(libs.plugins.kotlin.jvm) apply false
	alias(libs.plugins.kotlin.spring) apply false
	alias(libs.plugins.spring.boot) apply false
	alias(libs.plugins.kotlin.jpa) apply false
}

val kotlinJvmPluginId = libs.plugins.kotlin.jvm.get().pluginId

val springBootBom = libs.spring.boot.dependencies
val kotlinReflect = libs.kotlin.reflect
val kotlinTestJunit5 = libs.kotlin.test.junit5
val junitPlatformLauncher = libs.junit.platform.launcher

allprojects {
	group = "com.seaotter"
	version = "0.0.1-SNAPSHOT"
	description = "triggerly-claude"
}

subprojects {
	apply(plugin = kotlinJvmPluginId)
	apply(plugin = "java-library")

	extensions.configure<JavaPluginExtension> {
		toolchain {
			languageVersion = JavaLanguageVersion.of(21)
		}
	}

	extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
		compilerOptions {
			freeCompilerArgs.addAll(
				"-Xjsr305=strict",
				"-Xannotation-default-target=param-property",
			)
		}
	}

	dependencies {
		add("implementation", platform(springBootBom))
		add("testImplementation", platform(springBootBom))

		add("implementation", kotlinReflect)

		add("testImplementation", kotlinTestJunit5)
		add("testRuntimeOnly", junitPlatformLauncher)
	}

	tasks.withType<Test>().configureEach {
		useJUnitPlatform()
	}
}
