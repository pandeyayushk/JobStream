# JobStream

JobStream is an early-stage Java application scaffold for building job-stream workflows.

## What is it?

JobStream is intended to be a Java-based application for building job-stream workflows. At the moment, its implemented behavior is deliberately minimal: it starts from a single `Main` class and prints a startup message.

## Why am I building it?

This repository provides a clean starting point for developing JobStream incrementally: Maven supplies a standard project layout, Java 21 is the target runtime, and a test source set is already in place. The product workflow, integrations, and user-facing features have not been implemented yet.

## Current status

- Maven project configured as `io.github.pandeyayushk:JobStream:1.0-SNAPSHOT`.
- Java 21 configured in `pom.xml`.
- Application entry point: `io.github.pandeyayushk.jobstream.Main`.
- Current output: `JobStream starting...`.
- A smoke-test entry point exists at `src/test/java/.../MainTest.java`; it prints `Testing...` when run directly.
- `mvn test` currently succeeds, but executes **zero assertions/tests** because `MainTest` does not yet contain JUnit test methods.

## Requirements

- JDK 21
- Apache Maven 3.8 or newer

Confirm both are available:

```powershell
java -version
mvn -version
```

## How to build

From the repository root:

```powershell
mvn package
```

Maven compiles the application and creates a JAR under `target/`.

## How to test

Run the Maven test lifecycle:

```powershell
mvn test
```

To run the current standalone smoke-test class and see its output:

```powershell
mvn test-compile
java -cp target/test-classes io.github.pandeyayushk.jobstream.MainTest
```

## How to run

Compile the application, then invoke its entry point:

```powershell
mvn compile
java -cp target/classes io.github.pandeyayushk.jobstream.Main
```

Expected output:

```text
JobStream starting...
```
