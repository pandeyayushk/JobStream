# ADR-001: Use Java 21 LTS and Maven as the Foundation

**Status:** Accepted

## Context
- JobStream needs a stable, long-term-supported Java version
- The build system must be standard, well-documented, and widely supported
- The project is a learning-oriented but production-minded system

## Decision
- Java 21 LTS as the runtime (configured in pom.xml via maven.compiler.release=21)
- Apache Maven as the build system
- JUnit 5 as the testing framework
- Standard Maven directory layout (src/main/java, src/test/java)

## Alternatives Considered
- **Java 17 LTS**: Still supported but Java 21 offers virtual threads, pattern matching, and other features useful for a concurrent system
- **Gradle**: Powerful but Maven's convention-over-configuration and XML POM are more explicit for a learning project
- **TestNG**: Viable but JUnit 5 is the de facto standard with excellent tooling support

## Consequences
- Can leverage Java 21 features: virtual threads (Project Loom), pattern matching, sealed classes, records
- Maven's standard layout provides clear project structure conventions
- JUnit 5's extension model supports integration testing patterns needed later
- All team members and CI systems must have JDK 21 installed
