# AsyncExecutorUtil

## Overview

AsyncExecutorUtil is a Java utility for executing tasks asynchronously with support for dependencies, retries, and global handlers. It allows you to define tasks, specify dependencies, and execute them in the correct order while handling failures and retries.

## Features

- Asynchronous task execution
- Dependency management
- Retry policies (linear and nonlinear)
- Task success and failure callback
- Global success and failure handlers
- Fail-fast mechanism
- Circular dependency detection

## Requirements

- Java 21
- Maven 3.6.0 or higher

## Installation

Clone the repository and navigate to the project directory:

```sh
git clone https://github.com/debopampoddar/asyncexecutorutil.git
cd asyncexecutorutil
```

## Building the Project
```sh 
mvn clean install
```

## Usage
### Creating Tasks
To create a task, extend the `Task` class and implement the `execute` method. You can also specify dependencies and retry policies in the constructor.

```
Task<String> task1 = new Task.Builder<String>("task1")
        .action(() -> "Task 1 completed")
        .build();

Task<String> task2 = new Task.Builder<String>("task2")
        .dependencies("task1")
        .action(() -> "Task 2 completed")
        .build();
```
### Executing Tasks
To execute tasks, create an `TaskExecutor` instance and add tasks to it. Then, call the `execute` method to execute the tasks asynchronously.

```
ExecutionResult result = new TaskExecutor.Builder()
        .addTask(task1)
        .addTask(task2)
        .build()
        .execute();
```

### Handling Results
You can handle the results of task execution using the `ExecutionResult` object. The `getResults` method returns a map of task names to results.

```
Map<String, Object> results = result.getResults();
System.out.println(results.get("task1")); // Task 1 completed
System.out.println(results.get("task2")); // Task 2 completed
```

### Handling Failures
You can specify a global failure handler that will be called when any task fails. The handler can be used to log errors, send notifications, or perform other actions.

```
TaskExecutor executor = new TaskExecutor.Builder()
        .addTask(task1)
        .addTask(task2)
        .onFailure((task, exception) -> System.out.println("Task " + task.getName() + " failed: " + exception.getMessage()))
        .build();
```
### Handling Retries
You can specify a retry policy for a task using the `retry` method in the `Task.Builder`. The policy can be linear or nonlinear, with options for maximum retries and delay between retries.

```
Task<String> task = new Task.Builder<String>("task")
        .action(() -> {
            if (Math.random() < 0.5) {
                throw new RuntimeException("Task failed");
            }
            return "Task completed";
        })
        .retry(RetryPolicy.linear(3, Duration.ofSeconds(1)))
        .build();
```

## Examples
The `ExampleUsage` class demonstrates the usage of AsyncExecutorUtil with tasks, dependencies, retries, and global handlers. You can run the example using the following command:

```
ExecutionResult result = new TaskExecutor.Builder()
                .addTask(successTask("t1"))
                .build()
                .execute();
```
## Running Tests
To run the unit tests, execute:

```sh 
mvn test
```

The tests validate:
- Task execution order
- Dependency resolution
- Retry policies
- Failure handling
- Circular dependency detection
- Global success and failure handlers
- Fail-fast mechanism
- Asynchronous task execution

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing
Contributions are welcome! Please feel free to submit a pull request or open an issue if you encounter any problems.

## Contact
For any questions or feedback, please contact me at debopam.poddar@gmail.com


