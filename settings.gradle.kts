dependencyResolutionManagement {
  @Suppress("UnstableApiUsage")
  repositories {
    mavenCentral()
  }
}

rootProject.name = "triggerly-claude"

include(":triggerly-domain")
include(":triggerly-application")
include(":triggerly-adapter-in-web")
include(":triggerly-adapter-out-persistence")
include(":triggerly-adapter-messaging")
include(":triggerly-bootstrap")
include(":triggerly-sdk")
