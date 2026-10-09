package io.github.pasindu9999.orderflow.messaging.testing;

import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test {@link FaultInjector}: arm a point, and execution reaching it throws {@link InjectedFault}, a plain
 * (so retryable) runtime exception. Call {@link #reset()} after each test so a fault never leaks into the next one.
 */
public class ProgrammableFaultInjector implements FaultInjector {

    public static class InjectedFault extends RuntimeException {

        public InjectedFault(String point) {
            super("Injected fault at " + point);
        }
    }

    private final Map<String, AtomicInteger> callsUntilFailure = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresLeft = new ConcurrentHashMap<>();

    public void failOnce(String point) {
        failOnCall(point, 1);
    }

    /** Throws on the {@code call}-th time {@code point} is reached from now on (1 = the next time). */
    public void failOnCall(String point, int call) {
        callsUntilFailure.put(point, new AtomicInteger(call));
    }

    /** Throws the next {@code times} times {@code point} is reached, like a dependency that is down for a while. */
    public void failTimes(String point, int times) {
        failuresLeft.put(point, new AtomicInteger(times));
    }

    public void reset() {
        callsUntilFailure.clear();
        failuresLeft.clear();
    }

    @Override
    public void at(String point) {
        AtomicInteger remaining = callsUntilFailure.get(point);
        if (remaining != null && remaining.decrementAndGet() == 0) {
            callsUntilFailure.remove(point);
            throw new InjectedFault(point);
        }
        AtomicInteger left = failuresLeft.get(point);
        if (left != null && left.getAndDecrement() > 0) {
            throw new InjectedFault(point);
        }
    }
}
