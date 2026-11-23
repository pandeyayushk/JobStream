package io.github.pandeyayushk.jobstream.api;

/** Checks availability of a dependency required for API readiness. */
@FunctionalInterface
public interface ReadinessCheck {
    void check() throws Exception;
}
