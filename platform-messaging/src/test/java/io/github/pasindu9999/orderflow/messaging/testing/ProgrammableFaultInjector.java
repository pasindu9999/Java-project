package io.github.pasindu9999.orderflow.messaging.testing;

import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link FaultInjector}: arm a point, and execution reaching it throws {@link InjectedFault}, a plain
 * (so retryable) runtime exception, or runs an action. Call {@link #reset()} after each test so a fault never
 * leaks into the next one.
 */
public class ProgrammableFaultInjector implements FaultInjector {

    public static class InjectedFault extends RuntimeException {

        public InjectedFault(String point) {
            super("Injected fault at " + point);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProgrammableFaultInjector.class);

    private final Map<String, AtomicInteger> callsUntilFailure = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresLeft = new ConcurrentHashMap<>();
    private final Map<String, Runnable> actions = new ConcurrentHashMap<>();

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

    /**
     * Runs {@code action} the next time {@code point} is reached, on the calling thread, e.g. to simulate a
     * concurrent writer. To act outside the caller's transaction, the action must hop to another thread.
     */
    public void runOnce(String point, Runnable action) {
        actions.put(point, action);
    }

    /** False once every arming of {@code point} has fired: lets a test prove its fault really happened. */
    public boolean isArmed(String point) {
        AtomicInteger left = failuresLeft.get(point);
        return callsUntilFailure.containsKey(point) || actions.containsKey(point) || (left != null && left.get() > 0);
    }

    public void reset() {
        callsUntilFailure.clear();
        failuresLeft.clear();
        actions.clear();
    }

    @Override
    public void at(String point) {
        Runnable action = actions.remove(point);
        if (action != null) {
            log.warn("Running injected action at {}", point);
            action.run();
        }
        AtomicInteger remaining = callsUntilFailure.get(point);
        if (remaining != null && remaining.decrementAndGet() == 0) {
            callsUntilFailure.remove(point);
            throw fault(point);
        }
        AtomicInteger left = failuresLeft.get(point);
        if (left != null && left.getAndDecrement() > 0) {
            throw fault(point);
        }
    }

    private static InjectedFault fault(String point) {
        log.warn("Injecting fault at {}", point);
        return new InjectedFault(point);
    }
}
