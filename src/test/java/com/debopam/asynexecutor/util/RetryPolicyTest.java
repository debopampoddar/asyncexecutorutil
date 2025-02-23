package com.debopam.asynexecutor.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {
    private static final Duration BASE_INTERVAL = Duration.ofMillis(10);

    @Test
    void nonePolicy_NoRetries() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = RetryPolicy.none();

        Callable<String> action = () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Fail");
        };

        assertThrows(RuntimeException.class, () -> policy.execute(action));
        assertEquals(1, attempts.get());
    }

    @Test
    void linearPolicy_SuccessAfterRetries() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy.Linear(3, BASE_INTERVAL, e -> true);

        String result = policy.execute(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("Retry");
            }
            return "Success";
        });

        assertEquals(3, attempts.get());
        assertEquals("Success", result);
    }

    @Test
    void linearPolicy_FailureAfterMaxRetries() {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy.Linear(2, BASE_INTERVAL, e -> true);

        assertThrows(RuntimeException.class, () -> policy.execute(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Fail");
        }));

        assertEquals(3, attempts.get()); // 1 initial + 2 retries
    }

    @Test
    void linearPolicy_RespectsRetryCondition() {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy.Linear(3, BASE_INTERVAL,
                e -> e instanceof IOException);

        assertThrows(RuntimeException.class, () -> policy.execute(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Non-retryable");
        }));

        assertEquals(1, attempts.get());
    }

    @Test
    void nonlinearPolicy_FibonacciBackoffTiming() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy.Nonlinear(3, BASE_INTERVAL, e -> true);

        long start = System.currentTimeMillis();
        assertThrows(RuntimeException.class, () -> policy.execute(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Fail");
        }));
        long duration = System.currentTimeMillis() - start;

        assertEquals(4, attempts.get()); // 1 initial + 3 retries
        assertTrue(duration >= 40); // 10 + 10 + 20ms (Fibonacci sequence: 1,1,2)
    }

    @Test
    void nonlinearPolicy_RetryConditionFailure() {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy.Nonlinear(3, BASE_INTERVAL,
                e -> e.getMessage().contains("Retryable"));

        assertThrows(RuntimeException.class, () -> policy.execute(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Non-retryable");
        }));

        assertEquals(1, attempts.get());
    }

    @Test
    void policyPropagatesOriginalException() {
        RetryPolicy policy = new RetryPolicy.Linear(2, BASE_INTERVAL, e -> true);
        Exception expected = new IOException("Original error");

        Exception actual = assertThrows(Exception.class, () -> policy.execute(() -> {
            throw expected;
        }));

        assertSame(expected, actual);
    }

    @Test
    void invalidPolicyConfiguration() {
        // Negative maxRetries
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy.Linear(-1, BASE_INTERVAL, e -> true));

        // Null condition
        assertThrows(NullPointerException.class,
                () -> new RetryPolicy.Linear(1, BASE_INTERVAL, null));

        // Negative interval
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy.Nonlinear(1, Duration.ofMillis(-1), e -> true));
    }

    @Test
    void zeroRetriesBehavior() throws Exception {
        RetryPolicy policy = new RetryPolicy.Linear(0, BASE_INTERVAL, e -> true);
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(RuntimeException.class, () -> policy.execute(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException();
        }));

        assertEquals(1, attempts.get());
    }

    @Test
    void successOnFirstAttempt() throws Exception {
        RetryPolicy policy = new RetryPolicy.Nonlinear(3, BASE_INTERVAL, e -> true);
        String result = policy.execute(() -> "Success");
        assertEquals("Success", result);
    }

    @Test
    void interruptedRetryHandling() {
        RetryPolicy policy = new RetryPolicy.Linear(3, Duration.ofSeconds(1), e -> true);
        Thread.currentThread().interrupt();

        assertThrows(InterruptedException.class, () -> policy.execute(() -> {
            throw new IOException();
        }));
    }
}
