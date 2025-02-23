package com.debopam.asynexecutor.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Executes tasks with dependency management, parallel execution of independent tasks,
 * and configurable error handling strategies.
 */
public class TaskExecutor {
    private final List<Task<?>> tasks;
    private final boolean failFast;
    private final Runnable globalSuccessHandler;
    private final Consumer<Exception> globalFailureHandler;

    private TaskExecutor(Builder builder) {
        this.tasks = List.copyOf(builder.tasks);
        this.failFast = builder.failFast;
        this.globalSuccessHandler = builder.globalSuccessHandler;
        this.globalFailureHandler = builder.globalFailureHandler;
    }

    public static class Builder {
        private final List<Task<?>> tasks = new ArrayList<>();
        private final Set<String> taskIds = new HashSet<>();
        private boolean failFast = false;
        private Runnable globalSuccessHandler = () -> {};
        private Consumer<Exception> globalFailureHandler = e -> {};

        public Builder addTask(Task<?> task) {
            if (!taskIds.add(task.id())) {
                throw new IllegalArgumentException("Duplicate task ID: " + task.id());
            }
            tasks.add(task);
            return this;
        }

        public Builder failFast(boolean failFast) {
            this.failFast = failFast;
            return this;
        }

        public Builder globalSuccessHandler(Runnable handler) {
            this.globalSuccessHandler = handler;
            return this;
        }

        public Builder globalFailureHandler(Consumer<Exception> handler) {
            this.globalFailureHandler = handler;
            return this;
        }

        public TaskExecutor build() {
            if (tasks.isEmpty()) throw new IllegalStateException("No tasks provided");
            return new TaskExecutor(this);
        }

        public Builder addTasks(List<Task<String>> tasks) {
            Objects.requireNonNull(tasks, "Tasks cannot be null");
            tasks.forEach(task -> addTask(task));
            return this;
        }
    }

    /**
     * Execute all tasks according to configuration
     * @return ExecutionResult containing outcomes of all tasks
     * @throws IllegalStateException if validation fails (circular dependencies,
     *         missing dependencies, or empty task list)
     */
    public ExecutionResult execute() {
        validateDependencies();
        validateNoCycles();

        ExecutionResult result = new ExecutionResult();
        if (tasks.size() == 1 && tasks.get(0).dependencies().isEmpty()) {
            executeSynchronously(result);
        } else {
            try {
                executeAsynchronously(result);
            } catch (RuntimeException e) {
                ;
            }
        }
        return result;
    }

    /**
     * Executes a single task synchronously without threading
     *
     * @param result Container for storing execution results
     */
    private void executeSynchronously(ExecutionResult result) {
        Task task = tasks.get(0);
        AtomicBoolean failureFlag = new AtomicBoolean(false);
        executeTask((Task<Object>) task, result, failureFlag);
        triggerGlobalHandlers(result);
    }

    /**
     * Execute tasks asynchronously with virtual threads
     *
     * @param result Container for storing execution outcomes
     */
    private void executeAsynchronously(ExecutionResult result) {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Map<String, CompletableFuture<?>> futures = new ConcurrentHashMap<>();
        AtomicBoolean failureFlag = new AtomicBoolean(false);
        AtomicReference<Exception> firstError = new AtomicReference<>();

        // Create all task futures
        for (Task<?> task : topologicalSort()) {
            CompletableFuture<?> future = buildTaskFuture(task, futures, executor, result, failureFlag);
            futures.put(task.id(), future);

            // Configure fail-fast handling
            future.handle((res, ex) -> {
                if (ex != null) {
                    Exception cause = unwrapCompletionException(ex);

                    // Capture first error for global handler
                    if (firstError.compareAndSet(null, cause)) {
                        globalFailureHandler.accept(cause);
                    }

                    // Trigger fail-fast cancellation
                    if (failFast && failureFlag.compareAndSet(false, true)) {
                        cancelAllFutures(futures);
                        executor.shutdownNow();
                    }
                }
                return null;
            });
        }

        try {
            CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).join();
            if (!failureFlag.get()) {
                globalSuccessHandler.run();
            }
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Build CompletableFuture for a task with dependency chaining
     * @param task Task to execute
     * @param futures Map of existing futures for dependency lookup
     * @param executor Executor service for async operations
     * @param result Container for storing outcomes
     * @param failureFlag Atomic flag indicating failure state
     * @return CompletableFuture representing the task's execution chain
     */
    private CompletableFuture<?> buildTaskFuture(
            Task<?> task,
            Map<String, CompletableFuture<?>> futures,
            Executor executor,
            ExecutionResult result,
            AtomicBoolean failureFlag
    ) {
        List<? extends CompletableFuture<?>> dependencies = task.dependencies().stream()
                .map(futures::get)
                .toList();

        return CompletableFuture.allOf(dependencies.toArray(new CompletableFuture[0]))
                .thenApplyAsync(__ -> checkDependencies(dependencies), executor)
                .thenComposeAsync(__ -> CompletableFuture.supplyAsync(
                        () -> executeTask(task, result, failureFlag),
                        executor
                ), executor);
    }

    /**
     * Verify all dependencies completed successfully
     *
     * @param dependencies List of dependency futures
     * @throws CompletionException if any dependency failed
     */
    private Void checkDependencies(List<? extends CompletableFuture<?>> dependencies) {
        for (CompletableFuture<?> dep : dependencies) {
            if (dep.isCompletedExceptionally()) {
                throw new CompletionException("Dependency failed", null);
            }
        }
        return null;
    }

    /**
     * Execute individual task with retry policy and record results
     * @param task Task to execute
     * @param result Container for storing outcomes
     * @param failureFlag Atomic flag indicating failure state
     * @return Task result if successful
     * @throws CancellationException if execution was aborted
     */
    private <T> T executeTask(Task<T> task, ExecutionResult result, AtomicBoolean failureFlag) {
        if (failureFlag.get() || Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Execution aborted");
        }

        try {
            T value = task.retryPolicy().execute(task.action());
            result.recordSuccess(task.id(), value);
            task.onSuccess().accept(value);
            return value;
        } catch (Exception e) {
            return handleFailure(task, result, e, failureFlag);
        }
    }

    /**
     * Handle task failure and attempt fallback execution
     * @param task Failed task
     * @param result Container for storing outcomes
     * @param error Exception that caused failure
     * @param failureFlag Atomic flag indicating failure state
     * @return Fallback result if available
     * @throws CompletionException to propagate error for fail-fast handling
     */
    private <T> T handleFailure(Task<T> task, ExecutionResult result, Exception error, AtomicBoolean failureFlag) {
        try {
            if (task.fallback() != null) {
                T fallbackValue = task.fallback().call();
                result.recordSuccess(task.id(), fallbackValue);
                task.onSuccess().accept(fallbackValue);
                return fallbackValue;
            }
        } catch (Exception e) {
            error = e;
        }

        result.recordFailure(task.id(), error);
        task.onFailure().accept(error);

        // Propagate exception to trigger fail-fast
        if(failFast)
            throw new CompletionException(error);
        return null;
    }

    /**
     * Cancel all pending futures and interrupt running tasks
     * @param futures Map of task futures to cancel
     */
    private void cancelAllFutures(Map<String, CompletableFuture<?>> futures) {
        futures.values().forEach(future -> {
            if (!future.isDone()) {
                future.cancel(true);
            }
        });
    }

    /**
     * Unwrap root cause from CompletionException
     *
     * @param ex Throwable to unwrap
     * @return Original exception or wrapped Throwable
     */
    private Exception unwrapCompletionException(Throwable ex) {
        return (ex instanceof CompletionException && ex.getCause() instanceof Exception)
                ? (Exception) ex.getCause()
                : new Exception(ex);
    }

    /**
     * Validate all task dependencies exist in the task list
     *
     * @throws IllegalArgumentException if any dependency is missing
     */
    private void validateDependencies() {
        Set<String> validIds = tasks.stream()
                .map(Task::id)
                .collect(Collectors.toSet());

        for (Task<?> task : tasks) {
            for (String depId : task.dependencies()) {
                if (!validIds.contains(depId)) {
                    throw new IllegalArgumentException(
                            "Missing dependency: " + depId + " for task " + task.id()
                    );
                }
            }
        }
    }

    /**
     * Detect and prevent circular dependencies using DFS
     *
     * @throws IllegalStateException if circular dependency is found
     */
    private void validateNoCycles() {
        Map<String, List<String>> graph = tasks.stream()
                .collect(Collectors.toMap(Task::id, Task::dependencies));

        Set<String> visited = new HashSet<>();
        Set<String> stack = new HashSet<>();

        for (String node : graph.keySet()) {
            if (hasCycle(node, graph, visited, stack)) {
                throw new IllegalStateException("Circular dependency detected");
            }
        }
    }

    /** DFS cycle detection helper */
    private boolean hasCycle(String node, Map<String, List<String>> graph,
                             Set<String> visited, Set<String> stack) {
        if (stack.contains(node)) return true;
        if (visited.contains(node)) return false;

        visited.add(node);
        stack.add(node);

        for (String neighbor : graph.get(node)) {
            if (hasCycle(neighbor, graph, visited, stack)) {
                return true;
            }
        }

        stack.remove(node);
        return false;
    }

    /** Trigger global handlers based on execution outcome */
    private void triggerGlobalHandlers(ExecutionResult result) {
        if (result.isCompleteSuccess()) {
            globalSuccessHandler.run();
        } else if (!result.failures().isEmpty()) {
            Exception firstError = result.failures().values().iterator().next();
            globalFailureHandler.accept(firstError);
        }
    }

    /**
     * Topologically sort tasks using Kahn's algorithm
     *
     * @return Tasks in execution order (independent tasks first)
     * @throws IllegalStateException if cycle detected during sorting
     */
    private List<Task<?>> topologicalSort() {
        Map<String, Task<?>> taskMap = tasks.stream()
                .collect(Collectors.toMap(Task::id, t -> t));

        Map<String, List<String>> adjacencyList = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();

        // Initialize graph
        for (Task<?> task : tasks) {
            String taskId = task.id();
            adjacencyList.putIfAbsent(taskId, new ArrayList<>());
            inDegree.putIfAbsent(taskId, 0);

            for (String depId : task.dependencies()) {
                adjacencyList.computeIfAbsent(depId, k -> new ArrayList<>()).add(taskId);
                inDegree.merge(taskId, 1, Integer::sum);
            }
        }

        // Kahn's algorithm
        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        List<Task<?>> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String nodeId = queue.poll();
            sorted.add(taskMap.get(nodeId));

            for (String dependentId : adjacencyList.get(nodeId)) {
                inDegree.merge(dependentId, -1, Integer::sum);
                if (inDegree.get(dependentId) == 0) {
                    queue.add(dependentId);
                }
            }
        }

        if (sorted.size() != tasks.size()) {
            throw new IllegalStateException("Cycle detected despite validation");
        }

        return sorted;
    }
}
