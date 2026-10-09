package io.github.pasindu9999.orderflow.messaging;

/**
 * Named crash points in infrastructure code, so tests can simulate a process dying at an exact moment
 * (e.g. after a Kafka send but before the outbox row is marked).
 *
 * <p>Production uses {@link #NONE}. Tests register an implementation that throws at a chosen point. Production
 * code never checks "am I in a test?"; it only calls {@link #at}.
 */
@FunctionalInterface
public interface FaultInjector {

    FaultInjector NONE = point -> { };

    /** Called when execution reaches {@code point}. A test implementation throws here to simulate a crash. */
    void at(String point);
}
