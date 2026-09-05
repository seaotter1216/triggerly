plugins {
	id("maven-publish")
}

dependencies {
	implementation(libs.jackson.module.kotlin)
	testImplementation(libs.kotest.assertions.core)
}

publishing {
	publications {
		create<MavenPublication>("maven") {
			from(components["java"])
			groupId = project.group.toString()
			artifactId = "triggerly-sdk"
			version = project.version.toString()
		}
	}
	repositories {
		maven {
			name = "GitHubPackages"
			// TODO: YOUR_GITHUB_USERNAME 을 실제 GitHub 계정/조직명으로 교체
			url = uri("https://maven.pkg.github.com/YOUR_GITHUB_USERNAME/triggerly-claude")
			credentials {
				username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String?
				password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.token") as String?
			}
		}
	}
}
