package com.debopam.asynexecutor.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskExecutorTest {
    // Test Helpers
    private Task<String> successTask(String id) {
        return new Task.Builder<String>(id)
                .action(() -> "success")
                .build();
    }

    private Task<String> failingTask(String id, String errorMessage) {
        return new Task.Builder<String>(id)
                .action(() -> { throw new RuntimeException(errorMessage); })
                .build();
    }

    private Task<String> delayedFailingTask(String id, String errorMessage, Duration delay) {
        return new Task.Builder<String>(id)
                .action(() -> {
                    Thread.sleep(delay);
                    throw new RuntimeException(errorMessage);
                })
                .build();
    }

    // Basic Functionality
    @Test
    void singleTaskSuccess() {
        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(successTask("t1"))
                .build()
                .execute();

        assertEquals("success", result.getResult("t1").get());
        assertTrue(result.getExecutionOrder().contains("t1"));
    }

    @Test
    void independentParallelExecution() {
        AtomicBoolean t1Started = new AtomicBoolean();
        AtomicBoolean t2Started = new AtomicBoolean();

        Task<String> t1 = new Task.Builder<String>("t1")
                .action(() -> {
                    t1Started.set(true);
                    Thread.sleep(100);
                    return "t1";
                })
                .build();

        Task<String> t2 = new Task.Builder<String>("t2")
                .action(() -> {
                    t2Started.set(true);
                    Thread.sleep(100);
                    return "t2";
                })
                .build();

        new TaskExecutor.Builder()
                .addTask(t1)
                .addTask(t2)
                .build()
                .execute();

        assertTrue(t1Started.get());
        assertTrue(t2Started.get());
    }

    // Dependency Handling
    @Test
    void dependentTasksExecuteInOrder() {
        List<String> executionOrder = new ArrayList<>();

        Task<Boolean> t1 = new Task.Builder<Boolean>("t1")
                .action(() -> executionOrder.add("t1"))
                .build();

        Task<Boolean> t2 = new Task.Builder<Boolean>("t2")
                .dependencies("t1")
                .action(() -> executionOrder.add("t2"))
                .build();

        new TaskExecutor.Builder()
                .addTask(t1)
                .addTask(t2)
                .build()
                .execute();

        assertEquals(List.of("t1", "t2"), executionOrder);
    }

    // Retry Policies
    @Test
    void linearRetryWithFixedInterval() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        Task<String> task = new Task.Builder<String>("t1")
                .action(() -> {
                    if (attempts.incrementAndGet() < 3) throw new RuntimeException();
                    return "success";
                })
                .retryPolicy(new RetryPolicy.Linear(3, Duration.ofMillis(100), e -> true))
                .build();

        long start = System.currentTimeMillis();
        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(task)
                .build()
                .execute();

        assertTrue(System.currentTimeMillis() - start >= 200);
        assertEquals(3, attempts.get());
        assertEquals("success", result.getResult("t1").get());
    }

    @Test
    void nonlinearRetryWithFibonacciIntervals() {
        AtomicInteger attempts = new AtomicInteger();
        Task<String> task = new Task.Builder<String>("t1")
                .action(() -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException();
                })
                .retryPolicy(new RetryPolicy.Nonlinear(3, Duration.ofMillis(10), e -> true))
                .build();

        long start = System.currentTimeMillis();
        new TaskExecutor.Builder()
                .addTask(task)
                .build()
                .execute();

        long duration = System.currentTimeMillis() - start;
        assertTrue(duration >= 40); // 10 + 10 + 20ms
        assertEquals(4, attempts.get());
    }

    // Error Handling
    @Test
    void failFastCancelsAllTasks() throws InterruptedException {
        AtomicBoolean slowTaskRan = new AtomicBoolean();
        AtomicBoolean fastTaskFailed = new AtomicBoolean();

        Task<String> slowTask = new Task.Builder<String>("Slow")
                .action(() -> {
                    Thread.sleep(1000);
                    slowTaskRan.set(true);
                    return "Slow";
                })
                .build();

        Task<String> fastTask = new Task.Builder<String>("Fast")
                .action(() -> {
                    fastTaskFailed.set(true);
                    throw new RuntimeException("Fast failure");
                })
                .build();

        long start = System.currentTimeMillis();
        new TaskExecutor.Builder()
                .addTask(slowTask)
                .addTask(fastTask)
                .failFast(true)
                .build()
                .execute();

        long duration = System.currentTimeMillis() - start;

        assertTrue(duration < 500, "Execution should abort quickly");
        assertTrue(fastTaskFailed.get(), "Fast task should fail");
        assertFalse(slowTaskRan.get(), "Slow task should be cancelled");
    }

    @Test
    void dependencyFailureAbortsDependentTasks() {
        Task<String> a = failingTask("A", "Error");
        Task<String> b = successTask("B").<String>toBuilder().dependencies("A").build();
        Task<String> c = successTask("C").<String>toBuilder().dependencies("B").build();

        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(a)
                .addTask(b)
                .addTask(c)
                .failFast(true)
                .build()
                .execute();

        assertTrue(result.getException("A").isPresent());
        assertFalse(result.getResult("B").isPresent());
        assertFalse(result.getResult("C").isPresent());
    }

    @Test
    void globalHandlerReceivesFirstError() {
        AtomicReference<Exception> capturedError = new AtomicReference<>();

        Task<String> t1 = failingTask("T1", "First Error");
        Task<String> t2 = delayedFailingTask("T2", "Second Error", Duration.ofSeconds(5));

        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(t1)
                .addTask(t2)
                .failFast(true)
                .globalFailureHandler(capturedError::set)
                .build()
                .execute();

        assertTrue(capturedError.get().getMessage().contains("First Error"));
    }

    @Test
    void interruptedTaskThrowsCancellation() {
        AtomicBoolean cancellationOccurred = new AtomicBoolean(false);

        Task<String> task = new Task.Builder<String>("T1")
                .action(() -> {
                    try {
                        Thread.sleep(Duration.ofSeconds(5));
                    } catch (InterruptedException e) {
                        cancellationOccurred.set(true);
                        throw e;
                    }
                    return "Success";
                })
                .build();

        Task<String> trigger = new Task.Builder<String>("T2")
                .action(() -> { throw new RuntimeException("Fail fast"); })
                .build();

        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(task)
                .addTask(trigger)
                .failFast(true)
                .build()
                .execute();

        //System.out.println(result);

        assertNotNull(result);
        assertTrue(cancellationOccurred.getPlain());
    }

    @Test
    void fallbackExecutionOnFailure() {
        Task<String> task = new Task.Builder<String>("t1")
                .action(() -> { throw new RuntimeException(); })
                .fallback(() -> "fallback")
                .build();

        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(task)
                .build()
                .execute();

        assertEquals("fallback", result.getResult("t1").get());
    }

    // Edge Cases

    @Test
    void taskDependencyOnFailedTask() {
        Task<String> t1 = failingTask("t1", "error");
        Task<String> t2 = new Task.Builder<String>("t2")
                .dependencies("t1")
                .action(() -> "ok")
                .build();

        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(t1)
                .addTask(t2)
                .failFast(false)
                .build()
                .execute();

        assertTrue(result.getException("t1").isPresent());
        assertTrue(result.getResult("t2").isPresent());
        assertTrue(result.getException("t1").isPresent());
    }

    // Global Handlers
    @Test
    void globalSuccessHandlerCalled() {
        AtomicBoolean called = new AtomicBoolean();
        new TaskExecutor.Builder()
                .addTask(successTask("t1"))
                .globalSuccessHandler(() -> called.set(true))
                .build()
                .execute();
        assertTrue(called.get());
    }

    @Test
    void globalFailureHandlerCalled() {
        AtomicReference<Exception> captured = new AtomicReference<>();
        new TaskExecutor.Builder()
                .addTask(failingTask("t1", "error"))
                .globalFailureHandler(captured::set)
                .build()
                .execute();
        assertNotNull(captured.get());
        assertEquals("error", captured.get().getMessage());
    }

    @Test
    void executionOrderRespectsDependencies() {
        List<String> executionOrder = new ArrayList<>();

        // Independent tasks
        Task<Boolean> t1 = new Task.Builder<Boolean>("A")
                .action(() -> executionOrder.add("A"))
                .build();
        Task<Boolean> t2 = new Task.Builder<Boolean>("B")
                .action(() -> executionOrder.add("B"))
                .build();

        // Dependent tasks
        Task<Boolean> t3 = new Task.Builder<Boolean>("C")
                .dependencies("A")
                .action(() -> executionOrder.add("C"))
                .build();
        Task<Boolean> t4 = new Task.Builder<Boolean>("D")
                .dependencies("B", "C")
                .action(() -> executionOrder.add("D"))
                .build();

        new TaskExecutor.Builder()
                .addTask(t4)
                .addTask(t3)
                .addTask(t1)
                .addTask(t2)
                .build()
                .execute();

        // Verify independent tasks execute first
        assertTrue(executionOrder.indexOf("A") < executionOrder.indexOf("C"));
        assertTrue(executionOrder.indexOf("B") < executionOrder.indexOf("D"));

        // Verify all dependencies are respected
        assertTrue(executionOrder.indexOf("C") < executionOrder.indexOf("D"));
    }

    @Test
    void globalHandlersTriggerCorrectly() {
        AtomicBoolean successCalled = new AtomicBoolean();
        AtomicReference<Exception> failureCaptured = new AtomicReference<>();

        // Success case
        new TaskExecutor.Builder()
                .addTask(successTask("T1"))
                .globalSuccessHandler(() -> successCalled.set(true))
                .build()
                .execute();
        assertTrue(successCalled.get());

        // Failure case
        new TaskExecutor.Builder()
                .addTask(failingTask("T2", "error"))
                .globalFailureHandler(failureCaptured::set)
                .build()
                .execute();
        assertEquals("error", failureCaptured.get().getMessage());
    }

    @Test
    void retryPoliciesExecuteCorrectly() {
        AtomicInteger linearAttempts = new AtomicInteger();
        Task<String> linearTask = new Task.Builder<String>("Linear")
                .action(() -> {
                    if (linearAttempts.incrementAndGet() < 3) throw new RuntimeException();
                    return "success";
                })
                .retryPolicy(new RetryPolicy.Linear(3, Duration.ofMillis(100), e -> true))
                .build();

        long start = System.currentTimeMillis();
        ExecutionResult result = new TaskExecutor.Builder()
                .addTask(linearTask)
                .build()
                .execute();

        assertTrue(System.currentTimeMillis() - start >= 200);
        assertEquals(3, linearAttempts.get());
        assertEquals("success", result.getResult("Linear").get());
    }

    @Test
    void failFastAbortsExecution() {
        AtomicBoolean t2Executed = new AtomicBoolean(false);
        Task<String> t1 = failingTask("T1", "error");
        Task<Void> t2 = new Task.Builder<Void>("T2")
                .action(() -> {
                    Thread.sleep(Duration.ofSeconds(5));
                    t2Executed.set(true);
                    return null;
                })
                .build();

        new TaskExecutor.Builder()
                .addTask(t1)
                .addTask(t2)
                .failFast(true)
                .build()
                .execute();

        assertFalse(t2Executed.get());
    }

    @Test
    void circularDependencyThrowsException() {
        Task<String> t1 = new Task.Builder<String>("A")
                .dependencies("B")
                .action(() -> "ok")
                .build();
        Task<String> t2 = new Task.Builder<String>("B")
                .dependencies("A")
                .action(() -> "ok")
                .build();

        assertThrows(IllegalStateException.class, () ->
                new TaskExecutor.Builder()
                        .addTask(t1)
                        .addTask(t2)
                        .build()
                        .execute());
    }

    @Test
    void synchronousSuccess() {
        AtomicBoolean globalSuccessCalled = new AtomicBoolean();
        Task<String> task = successTask("t1");

        new TaskExecutor.Builder()
                .addTask(task)
                .globalSuccessHandler(() -> globalSuccessCalled.set(true))
                .build()
                .execute();

        assertTrue(globalSuccessCalled.get());
    }

    @Test
    void synchronousFailure() {
        AtomicBoolean globalFailureCalled = new AtomicBoolean();
        Task<String> task = failingTask("t1", "error");

        new TaskExecutor.Builder()
                .addTask(task)
                .globalFailureHandler(e -> globalFailureCalled.set(true))
                .build()
                .execute();

        assertTrue(globalFailureCalled.get());
    }

    @Test
    void synchronousFallbackFailurePropagation() {
        AtomicReference<Exception> capturedError = new AtomicReference<>();
        Task<String> task = new Task.Builder<String>("t1")
                .action(() -> { throw new RuntimeException("main error"); })
                .fallback(() -> { throw new RuntimeException("fallback error"); })
                .build();

        new TaskExecutor.Builder()
                .addTask(task)
                .globalFailureHandler(capturedError::set)
                .build()
                .execute();

        assertEquals("fallback error", capturedError.get().getMessage());
    }

    @Test
    void multiLevelDependencyExecutionOrder() {
        // Create tasks with System.out logging
        List<Task<String>> tasks = List.of(
                new Task.Builder<String>("A")
                        .action(() -> {
                            System.out.println("[START] Executing independent task A");
                            return "A";
                        })
                        .build(),

                new Task.Builder<String>("B")
                        .dependencies("A")
                        .action(() -> {
                            System.out.println("[START] Executing task B (depends on A)");
                            return "B";
                        })
                        .build(),

                new Task.Builder<String>("C")
                        .dependencies("B")
                        .action(() -> {
                            System.out.println("[START] Executing task C (depends on B)");
                            return "C";
                        })
                        .build(),

                new Task.Builder<String>("D")
                        .action(() -> {
                            System.out.println("[START] Executing independent task D");
                            return "D";
                        })
                        .build(),

                new Task.Builder<String>("E")
                        .dependencies("D")
                        .action(() -> {
                            System.out.println("[START] Executing task E (depends on D)");
                            return "E";
                        })
                        .build()
        );

        System.out.println("\nStarting test execution...");
        ExecutionResult result = new TaskExecutor.Builder()
                .addTasks(tasks)
                .build()
                .execute();
        System.out.println("Execution complete.\n");

        // Get actual execution order from result
        List<String> executionOrder = result.getExecutionOrder();
        System.out.println("Recorded execution order: " + executionOrder);

        // Verify execution constraints
        assertExecutionOrder(executionOrder,
                List.of("A", "D"),  // Independent tasks should complete first
                List.of("B", "E"),  // First-level dependencies next
                List.of("C")        // Final dependency last
        );
    }

    /**
     * Validates that tasks execute in the specified group order
     * @param actualOrder List of task IDs in execution order
     * @param expectedGroups Varargs of task groups in expected order
     */
    private void assertExecutionOrder(List<String> actualOrder, List<String>... expectedGroups) {
        int previousGroupMaxIndex = -1;

        for (List<String> group : expectedGroups) {
            // Verify all tasks in group exist in execution order
            assertTrue(actualOrder.containsAll(group),
                    "Missing tasks from group: " + group);

            int currentGroupMaxIndex = -1;

            // Check each task in current group
            for (String task : group) {
                int taskIndex = actualOrder.indexOf(task);

                // Verify task comes after all previous groups
                assertTrue(taskIndex > previousGroupMaxIndex,
                        "Task " + task + " at index " + taskIndex
                                + " should come after previous group (max index "
                                + previousGroupMaxIndex + ")");

                // Track max index in current group
                currentGroupMaxIndex = Math.max(currentGroupMaxIndex, taskIndex);
            }

            // Update previous group max for next iteration
            previousGroupMaxIndex = currentGroupMaxIndex;
        }
    }

    @Test
    void duplicateTaskIdThrowsException() {
        System.out.println("\nRunning duplicate task ID test...");

        Task<String> task1 = new Task.Builder<String>("T1")
                .action(() -> "Task 1")
                .build();

        Task<String> task2 = new Task.Builder<String>("T1")  // Duplicate ID
                .action(() -> "Task 2")
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            new TaskExecutor.Builder()
                    .addTask(task1)
                    .addTask(task2)  // Should throw here
                    .build();
        }, "Should throw when adding duplicate task IDs");

        System.out.println("Duplicate ID test passed: Exception thrown as expected");
    }

    @Test
    void missingDependencyThrowsException() {
        System.out.println("\nRunning missing dependency test...");

        Task<String> task = new Task.Builder<String>("T1")
                .dependencies("GHOST_TASK")  // Non-existent dependency
                .action(() -> {
                    System.out.println("This should never execute");
                    return "Result";
                })
                .build();

        TaskExecutor executor = new TaskExecutor.Builder()
                .addTask(task)
                .build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            executor.execute();  // Validation happens during execution
        }, "Should throw on missing dependency");

        assertTrue(exception.getMessage().contains("Missing dependency"),
                "Error message should indicate missing dependency");
        assertTrue(exception.getMessage().contains("GHOST_TASK"),
                "Error message should name missing task");

        System.out.println("Missing dependency test passed: " + exception.getMessage());
    }
}
