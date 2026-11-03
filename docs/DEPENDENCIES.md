# Dependency currency (KANBAN E7-3, E8-1)

Checked 2026-11-02 against Maven Central `maven-metadata.xml`, the Gradle services API, Docker Hub tags and the Adoptium release
list. "Latest" means the newest stable release published there on that date. Re-check with the commands at the bottom; versions
live in `build.gradle` and `gradle.properties` (the Micronaut BOM manages the rest).

| Component | Used | Latest | Notes |
|---|---|---|---|
| Java (build and runtime) | 25 (Temurin 25 in the image, JDK 25.0.5 locally) | 25 is the newest LTS | 26/27 exist but are not LTS; the Micronaut Gradle plugin 5.x itself needs a Java 25 JVM |
| Gradle | 9.8.2 | 9.8.2 | wrapper |
| Shadow plugin | `com.gradleup.shadow` 9.6.1 | 9.6.1 | |
| Micronaut platform (BOM) | 5.2.4 | 5.2.4 | brings Micronaut Core 5.2.20, Serde 3.2.6, Micrometer 6.1.0, Picocli 6.1.0, Groovy module 5.1.0, Reactor 3.8.7 |
| Micronaut application plugin | 5.0.2 | 5.0.2 | |
| Groovy | 5.0.8 (from the BOM) | 6.0.0 | **Deferred on purpose.** Groovy 6 is out, but the Micronaut 5.2.4 BOM and its Groovy module are built on Groovy 5.0.8 and Spock 2.5 (for Groovy 6) is not what the BOM uses. Moving off the BOM's Groovy means leaving the supported combination. Revisit when a Micronaut release adopts Groovy 6 |
| Spock | 2.4-groovy-5.0 | 2.5-groovy-6.0 | follows Groovy; the build already matches the BOM's `spock.version` |
| Neo4j Java driver | 6.3.0 | 6.3.0 | |
| Neo4j server image | 2026.10.1 (Community and Enterprise) | 2026.10.1 | tags `latest` and `2026.10.1`; the pinned benchmark image is by digest |
| RabbitMQ client (`amqp-client`) | 5.37.0 | 5.37.0 | |
| `reactor-rabbitmq` | 1.5.6 | 1.5.6 | |
| Gson | 2.15.0 | 2.15.0 | |
| `commons-lang3` | 3.21.0 | 3.21.0 | |
| `everit-json-schema` | 1.14.6 | 1.14.6 | |
| Awaitility | 4.3.0 | 4.3.0 | test |
| Testcontainers | 2.0.5 | 2.0.5 | test |
| RabbitMQ image (dev compose, tests) | `management-alpine` (floating) | 4.4.0 | floating tag: it follows the newest release |
| Runtime base image | `eclipse-temurin:25-jre` | 25.0.5 | |

## What was done on 2026-11-02
Gradle 8.14.3 -> 9.8.2, Shadow 8.3.10 -> 9.6.1, Micronaut platform 4.10.22 -> 5.2.4, Micronaut application plugin 4.6.2 -> 5.0.2,
Java 21 -> 25 (build, Dockerfile, GitHub workflow, Jenkinsfile), Groovy 4 -> 5.0.8, Spock 2.4-groovy-4.0 -> 2.4-groovy-5.0,
Gradle DSL property assignments (`version = ...`, `zip64 = true`). No source change was needed. The full suite passes unchanged:
199 tests, 0 failures, 10 skipped. Gradle still prints two deprecation warnings that come from the Micronaut plugin (IDE tasks,
project-as-dependency notation), not from this build; they matter for Gradle 10.

## Not done
* Groovy 6 / Spock 2.5 (see above).
* A recurring check (Dependabot or Renovate) is not set up; the commands below are the manual one.
* The `lib/*.jar` files, `micronaut-cli.yml` and the Kubernetes manifest are unused or stale and untouched (E8-4).

## Re-checking
```
curl -s https://repo1.maven.org/maven2/<group path>/<artifact>/maven-metadata.xml | grep -E '<(release|latest)>'
curl -s https://services.gradle.org/versions/current | grep '"version"'
curl -s 'https://hub.docker.com/v2/repositories/library/neo4j/tags?page_size=50&ordering=last_updated'
./gradlew dependencies --configuration runtimeClasspath
```
