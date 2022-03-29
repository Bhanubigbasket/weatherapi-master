import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
  java
  application
  id("com.github.johnrengelman.shadow") version "7.0.0"
}

group = "com.demo"
version = "1.0.0-SNAPSHOT"

repositories {
  mavenCentral()
}

val vertxVersion = "4.2.3"
val junitJupiterVersion = "5.7.0"

val mainVerticleName = "com.demo.weatherApp.MainVerticle"
val launcherClassName = "io.vertx.core.Launcher"

val watchForChange = "src/**/*"
val doOnChange = "${projectDir}/gradlew classes"

application {
  mainClass.set(launcherClassName)
}

dependencies {
  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
  implementation("io.vertx:vertx-web-client")
  implementation("io.vertx:vertx-web")
  implementation("io.vertx:vertx-mysql-client")
  implementation(platform("org.testcontainers:testcontainers-bom:1.16.3"))
  implementation("io.vertx:vertx-web")
  implementation("io.vertx:vertx-unit:4.2.6")
  implementation("io.vertx:vertx-micrometer-metrics:4.2.6")
  implementation("io.micrometer:micrometer-registry-influx:1.5.14")
  testImplementation("io.vertx:vertx-junit5")
  testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
  testImplementation ("org.testcontainers:testcontainers:1.16.3")
  testImplementation("org.testcontainers:mysql") //no version specified
  testImplementation("org.assertj:assertj-core:3.21.0")
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<ShadowJar> {
  archiveClassifier.set("fat")
  manifest {
    attributes(mapOf("Main-Verticle" to mainVerticleName))
  }
  mergeServiceFiles()
}

tasks.withType<Test> {
  useJUnitPlatform()
  testLogging {
    events = setOf(PASSED, SKIPPED, FAILED)
  }
}
tasks.withType<JavaExec> {
  args = listOf("run", mainVerticleName, "--redeploy=$watchForChange", "--launcher-class=$launcherClassName", "--on-redeploy=$doOnChange")
}
