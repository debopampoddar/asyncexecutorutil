package com.debopam.asynexecutor.util;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.lang.Thread.sleep;

/**
 * Handles task retry logic with configurable strategies
 */
public abstract class RetryPolicy {
    protected final int maxRetries;
    protected final Predicate<Exception> condition;
    protected final Duration baseInterval;

    /**
     * @param maxRetries   Maximum number of retry attempts
     * @param baseInterval Base wait time between attempts
     * @param condition    Predicate to determine if exception is retryable
     */
    protected RetryPolicy(int maxRetries, Duration baseInterval, Predicate<Exception> condition) {
        requireCondition(maxRetries >= 0, new IllegalArgumentException("Max Retries should be non negative"));
        requireCondition(!baseInterval.isNegative(), new IllegalArgumentException("Interval should be positive"));

        this.maxRetries = maxRetries;
        this.baseInterval = baseInterval;
        this.condition = Objects.requireNonNull(condition);
    }

    /**
     * Executes the action with retry logic
     *
     * @return Result of successful execution
     * @throws Exception Last encountered exception if all retries fail
     */
    public abstract <T> T execute(Callable<T> action) throws Exception;

    /**
     * Checks if condition satisfies or not. If not throws the exception
     *
     * @param condition Condition to check
     * @param exception Exception to be thrown if condition not satisfies
     * @param <T>
     */
    public static <T extends RuntimeException> void requireCondition(boolean condition, T exception) {
        if (!condition) {
            throw exception;
        }
    }

    /**
     * No retry policy - fails immediately on error
     */
    public static class None extends RetryPolicy {
        public None() {
            super(0, Duration.ZERO, e -> false);
        }

        @Override
        public <T> T execute(Callable<T> action) throws Exception {
            return action.call();
        }
    }

    /**
     * Linear retry policy - fixed intervals between attempts
     */
    public static class Linear extends RetryPolicy {
        public Linear(int maxRetries, Duration interval, Predicate<Exception> condition) {
            super(maxRetries, interval, condition);
        }

        @Override
        public <T> T execute(Callable<T> action) throws Exception {
            Exception lastError = null;
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    return action.call();
                } catch (Exception e) {
                    lastError = e;
                    if (attempt >= maxRetries || !condition.test(e)) break;
                    sleep(baseInterval);
                }
            }
            throw lastError;
        }

        private void sleep(Duration duration) throws InterruptedException {
            if (!duration.isZero()) {
                TimeUnit.MILLISECONDS.sleep(duration.toMillis());
            }
        }
    }

    /**
     * Nonlinear retry policy - increasing intervals (Fibonacci sequence)
     */
    public static class Nonlinear extends RetryPolicy {
        private long prev = 0;
        private long current = 1;

        public Nonlinear(int maxRetries, Duration baseInterval, Predicate<Exception> condition) {
            super(maxRetries, baseInterval, condition);
        }

        @Override
        public <T> T execute(Callable<T> action) throws Exception {
            Exception lastError = null;
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    return action.call();
                } catch (Exception e) {
                    lastError = e;
                    if (attempt >= maxRetries || !condition.test(e)) break;
                    advanceFibonacci();
                    sleep(Duration.ofMillis(current * baseInterval.toMillis()));
                }
            }
            throw lastError;
        }

        private void advanceFibonacci() {
            long next = prev + current;
            prev = current;
            current = next;
        }
    }

    public static RetryPolicy none() {
        return new None();
    }
}
