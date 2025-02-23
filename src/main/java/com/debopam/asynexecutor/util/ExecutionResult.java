package com.debopam.asynexecutor.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Captures and provides access to task execution results
 */
public record ExecutionResult(Map<String, Object> successes,
                              Map<String, Exception> failures,
                              List<String> executionOrder) {

    public ExecutionResult() {
        this(new HashMap<>(), new HashMap<>(), new ArrayList<>());
    }

    void recordSuccess(String taskId, Object result) {
        successes.put(taskId, result);
        executionOrder.add(taskId);
    }

    void recordFailure(String taskId, Exception error) {
        failures.put(taskId, error);
        executionOrder.add(taskId);
    }

    public Optional<Object> getResult(String taskId) {
        return Optional.ofNullable(successes.get(taskId));
    }

    public Optional<Exception> getException(String taskId) {
        return Optional.ofNullable(failures.get(taskId));
    }

    public List<String> getExecutionOrder() {
        return Collections.unmodifiableList(executionOrder);
    }

    /**
     * @return true if all tasks completed successfully
     */
    public boolean isCompleteSuccess() {
        return failures.isEmpty();
    }


}
