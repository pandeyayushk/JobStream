# ADR-012: Use the JDK HTTP Server for the Phase 9 Operator API

- **Status:** Accepted for Phase 9 implementation
- **Date:** 2026-11-23
- **Deciders:** JobStream maintainers

## Context

JobStream targets Java 21 and currently has no HTTP framework. It already uses Jackson 3.2.2 (`tools.jackson`) and has a bounded operator API endpoint list. The old Phase 9 proposal suggested Javalin 6.1.3, which is no longer the current maintained line.

## Decision

Use `com.sun.net.httpserver.HttpServer` from the JDK `jdk.httpserver` module, with an explicit bounded executor, route dispatcher, JSON serialization through the existing Jackson 3 mapper, validation/error mapping, request-size enforcement, and clean lifecycle.

Javalin's current 7.2.4 line supports Java 17+, so it is compatible with Java 21. Jackson 2 (`com.fasterxml.jackson`) and Jackson 3 (`tools.jackson`) are distinct APIs, but Javalin 7.2.0+ provides an optional native `JavalinJackson3` mapper. Javalin core's Jackson artifacts are optional; its `javalin-bundle` includes Jackson/Logback/testing utilities. Core still adds required Jetty runtime dependencies. Javalin could use its native Jackson 3 mapper without an adapter, while the JDK server adds no framework dependency. Spring and other larger alternatives add more surface than the fixed route set needs.

The current JobStream pin is Jackson 3.2.2. As of 2026-11-23, that minor line is closed; Jackson 3.3 is the active release branch and 3.1 is the LTS branch. Before implementation, move to a supported Jackson 3 line and verify the dependency tree. This maintenance action is independent of HTTP server selection.

## Consequences

- No new HTTP framework dependency or HTTP/Jackson version conflict.
- The project owns routing, JSON body binding, validation, error mapping, executor bounds, and lifecycle tests; these must be kept in shared API utilities and covered by HTTP integration tests.
- The JDK server is intentionally a small embedded server, not a full REST framework. Revisit if requirements expand substantially.
- Bind loopback by default. Remote binding is explicit and still requires an authenticated trusted edge because v1 has no built-in authentication.

## References

- [JDK 21 `jdk.httpserver`](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.httpserver/module-summary.html)
- [Javalin documentation](https://javalin.io/documentation) and [releases](https://github.com/javalin/javalin/releases)
- [Jackson version support](https://github.com/FasterXML/jackson/wiki/Jackson-Releases)
