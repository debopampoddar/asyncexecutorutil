package com.debopam.asynexecutor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * Represents an executable task with configurable behavior
 */
public record Task<T>(String id,
                      Callable<T> action,
                      Consumer<T> onSuccess,
                      Consumer<Exception> onFailure,
                      List<String> dependencies,
                      RetryPolicy retryPolicy,
                      Callable<T> fallback) {

    public static <T> Builder<T> builder(String id) {
        return new Builder<T>(id);
    }

    public <T> Builder<T> toBuilder() {
            Builder builder = new Builder(this.id());
            builder.action = this.action();
            builder.onSuccess = this.onSuccess();
            builder.onFailure = this.onFailure();
            builder.dependencies = this.dependencies();
            builder.retryPolicy = this.retryPolicy();
            builder.fallback = this.fallback();
            return builder;
    }

    public static class Builder<T> {
        private final String id;
        private Callable<T> action;
        private Consumer<T> onSuccess = t -> {
        };
        private Consumer<Exception> onFailure = e -> {
        };
        private List<String> dependencies = new ArrayList<>();
        private RetryPolicy retryPolicy = RetryPolicy.none();
        private Callable<T> fallback;

        public Builder(String id) {
            this.id = Objects.requireNonNull(id);
        }

        public Builder<T> action(Callable<T> action) {
            this.action = action;
            return this;
        }

        public Builder<T> onSuccess(Consumer<T> onSuccess) {
            this.onSuccess = onSuccess;
            return this;
        }

        public Builder<T> onFailure(Consumer<Exception> onFailure) {
            this.onFailure = onFailure;
            return this;
        }

        public Builder<T> dependencies(String... dependencies) {
            this.dependencies.addAll(Arrays.asList(dependencies));
            return this;
        }

        public Builder<T> retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder<T> fallback(Callable<T> fallback) {
            this.fallback = fallback;
            return this;
        }

        public Task<T> build() {
            Objects.requireNonNull(action, "Action must be specified");
            return new Task<>(this.id,
                    this.action,
                    this.onSuccess,
                    this.onFailure,
                    this.dependencies,
                    this.retryPolicy,
                    this.fallback);
        }
    }
}
