# CS324 Distributed Computing System

## Overview

This project implements a distributed computing system using Java RMI.

The system consists of:

- Bootstrap Node
- Multiple Worker Nodes
- Unstructured Worker Network
- Leader Election
- Job Allocation Counter (JAC)
- Distributed Job Processing
- Java Multithreading
- Client GUI
- CSV File Input
- Concurrent Tasks
- Multiple Clients

The system supports three computational jobs:

- `MAX(numbers)`
- `PRIMESUM(start, end)`
- `PRIMECOUNT(numbers)`

---

# Requirements

The following software is required:

- Java JDK
- IntelliJ IDEA
- Git

---

# Project Components

## Bootstrap Node

The Bootstrap Node provides the starting point for Worker discovery.

Its responsibilities include:

- Running as a separate Java process
- Maintaining information about active Workers
- Registering new Workers
- Unregistering Workers when they shut down
- Providing an existing active Worker to newly joined Workers
- Communicating using Java RMI

The Bootstrap Node does not participate in leader elections or computational job processing.

---

## Worker Nodes

Each Worker Node:

- Runs as a separate Java process
- Has a unique Worker ID
- Has its own RMI Registry port
- Registers with the Bootstrap Node
- Connects to another active Worker
- Maintains Worker neighbours
- Participates in leader elections
- Maintains a Job Allocation Counter (JAC)
- Processes distributed computational jobs
- Supports concurrent job execution using Java threads

Workers form an unstructured network and may have different numbers of neighbours.

---

# Leader Election

When a leader election is started:

1. A unique election ID is created.
2. An `ELECTION` message is propagated through the Worker network.
3. Each Worker processes the same election only once.
4. All reachable active Workers are considered.
5. The Worker with the lowest Job Allocation Counter (JAC) is selected.
6. If multiple Workers have the same lowest JAC, the Worker with the highest Worker ID is selected.
7. A `COORDINATOR` message is propagated through the Worker network.
8. All reachable Workers store the same Coordinator ID.

Duplicate election and Coordinator messages are prevented using thread-safe sets that store previously processed message IDs.

---

# Job Allocation Counter

Each Worker maintains a Job Allocation Counter called JAC.

The JAC is used during leader election.

When a Worker is acting as the Coordinator and assigns work to another Worker, its JAC is incremented.

During election:

- Lowest JAC wins.
- If JAC values are equal, the Worker with the highest ID wins.

This helps distribute the Coordinator role between Workers.

---

# Coordinator

One Worker acts as the Coordinator during each term.

The Coordinator:

- Accepts computational jobs from Clients
- Gets the list of active Workers
- Divides jobs between available Workers
- Sends partial jobs to Workers using Java RMI
- Collects partial results
- Combines the partial results
- Returns the final answer to the Client

A Coordinator can handle a maximum of five complete jobs during one term.

After the fifth job, a new leader election is automatically started.

---

# Supported Jobs

## MAX

`MAX(numbers)` finds the largest value in an unsorted list of numbers.

Example:

```text
MAX(12, 45, 7, 99, 23, 56)
```

Result:

```text
99
```

---

## PRIMESUM

`PRIMESUM(start, end)` calculates the sum of all prime numbers within a given range.

Example:

```text
PRIMESUM(1, 10)
```

Prime numbers:

```text
2, 3, 5, 7
```

Result:

```text
17
```

---

## PRIMECOUNT

`PRIMECOUNT(numbers)` counts how many prime numbers are present in an unsorted list.

Example:

```text
PRIMECOUNT(2, 4, 5, 7, 9, 11, 12)
```

Prime numbers:

```text
2, 5, 7, 11
```

Result:

```text
4
```

---

# Multithreading

Java threads are used throughout the system to support concurrent processing.

The Coordinator uses an `ExecutorService` to distribute partial jobs to multiple Workers.

Each Worker also uses an `ExecutorService` to process computational tasks.

The Client GUI uses its own thread pool so that RMI communication and job processing do not freeze the graphical interface.

Thread-safe structures used in the system include:

- `ConcurrentHashMap`
- Concurrent sets
- `AtomicInteger`
- `ExecutorService`
- `Future`

---

# Client GUI

The Client provides a Java Swing graphical interface.

The user can:

- Select a job type
- Enter numbers manually
- Enter a PRIMESUM range
- Load data from a CSV file
- Submit computational jobs
- View the result
- View the Worker that acted as Coordinator
- View task activity
- Submit multiple tasks
- Run multiple Client processes

The Client communicates with the Coordinator using Java RMI.

---

# How to Run the System

The system should be started in the following order.

---

## Step 1 - Start Bootstrap Node

Open:

```text
src/cs324/bootstrap/BootstrapNode.java
```

Run:

```text
BootstrapNode
```

The Bootstrap Node uses:

```text
RMI Registry Port: 1099
Service Name: BootstrapService
```

Expected output should indicate:

```text
Bootstrap Node is running.
```

Keep the Bootstrap Node running.

---

## Step 2 - Start Worker 1

Run `WorkerNode` using the following program arguments:

```text
1 2001
```

This creates:

```text
Worker ID: 1
Registry Port: 2001
```

Keep Worker 1 running.

---

## Step 3 - Start Worker 2

Run another `WorkerNode` process using:

```text
2 2002
```

This creates:

```text
Worker ID: 2
Registry Port: 2002
```

Worker 2 registers with the Bootstrap Node and connects to an existing Worker.

Keep Worker 2 running.

---

## Step 4 - Start Worker 3

Run another `WorkerNode` process using:

```text
3 2003
```

This creates:

```text
Worker ID: 3
Registry Port: 2003
```

Worker 3 registers and joins the Worker network.

Keep Worker 3 running.

---

## Step 5 - Start Leader Election

For testing, run:

```text
ElectionTest
```

The Workers participate in the leader-election process.

The Worker with the lowest JAC is elected as Coordinator.

If all Workers initially have the same JAC, the Worker with the highest Worker ID is selected.

All Workers should eventually report the same Coordinator.

---

## Step 6 - Start Client GUI

Open:

```text
src/cs324/client/ClientGUI.java
```

Run:

```text
ClientGUI
```

The graphical Client application will open.

The Client finds the current Coordinator and submits jobs using Java RMI.

---

# Using the Client GUI

## MAX

Select:

```text
MAX
```

Enter:

```text
12, 45, 7, 99, 23, 56
```

Click:

```text
Submit Job
```

Expected result:

```text
99
```

---

## PRIMESUM

Select:

```text
PRIMESUM
```

Enter:

```text
Start: 1
End: 10
```

Click:

```text
Submit Job
```

Expected result:

```text
17
```

---

## PRIMECOUNT

Select:

```text
PRIMECOUNT
```

Enter:

```text
2, 4, 5, 7, 9, 11, 12
```

Click:

```text
Submit Job
```

Expected result:

```text
4
```

---

# Loading Data From CSV

The Client can load numerical input from CSV files.

## MAX CSV Example

Create a file named:

```text
numbers.csv
```

Example contents:

```text
15,8,70,22,101,4
```

In the Client GUI:

1. Select `MAX`.
2. Click `Load CSV`.
3. Select the CSV file.
4. Confirm the numbers appear in the input field.
5. Click `Submit Job`.

Expected result:

```text
101
```

---

## PRIMECOUNT CSV Example

CSV contents:

```text
2,3,4,5,8,13,20
```

Select:

```text
PRIMECOUNT
```

Load the CSV and submit the job.

Expected result:

```text
4
```

---

## PRIMESUM CSV Example

A PRIMESUM CSV can contain:

```text
1,10
```

The first number is used as:

```text
Start = 1
```

The second number is used as:

```text
End = 10
```

Expected result:

```text
17
```

---

# Distributed Job Processing

The Coordinator divides each submitted job as evenly as possible among available Workers.

For example:

```text
PRIMESUM(1, 1000)
```

with four Workers can be divided into:

```text
Worker 1: PRIMESUM(1, 250)
Worker 2: PRIMESUM(251, 500)
Worker 3: PRIMESUM(501, 750)
Worker 4: PRIMESUM(751, 1000)
```

Each Worker calculates its assigned partial result.

The Coordinator waits for the partial results and combines them to produce the final answer.

---

# Multiple Concurrent Jobs

A single Client can submit multiple tasks.

The Client uses a thread pool so distributed requests can execute without freezing the GUI.

Example jobs:

```text
MAX
5,20,80,3
```

Expected:

```text
80
```

Another task can also be submitted:

```text
PRIMECOUNT
2,3,4,5,8,11
```

Expected:

```text
4
```

The activity section of the GUI shows submitted and completed jobs.

---

# Multiple Clients

The system supports multiple Clients running simultaneously.

Run:

```text
ClientGUI
```

more than once to create separate Client processes.

Example:

```text
Client 1
MAX
12,45,7,99
```

Expected:

```text
99
```

At the same time:

```text
Client 2
PRIMESUM
1 to 20
```

Expected:

```text
77
```

Both Clients can communicate with the distributed system simultaneously.

---

# Coordinator Term

Each Coordinator serves for one term.

A term allows a maximum of:

```text
5 jobs
```

After the fifth complete job:

1. The current Coordinator finishes the job.
2. The Coordinator term ends.
3. A new leader election starts.
4. Workers compare their JAC values.
5. A new Coordinator is selected.
6. The Coordinator announcement is propagated through the Worker network.

---

# Duplicate Election Protection

The Worker network may contain cycles, meaning the same election message could reach a Worker through different paths.

Each election therefore has a unique ID.

Workers maintain a thread-safe set containing IDs they have already processed.

If a Worker receives an election ID that it has already seen, it ignores the duplicate message.

This prevents:

- Repeated election processing
- Infinite circulation of messages
- Unnecessary Java RMI calls
- Wasted CPU and network resources
- Incorrect election behaviour

The same approach is used for Coordinator announcements.

---

# Error Handling

The system includes error handling for situations such as:

- Invalid Client input
- Empty input
- Incorrect PRIMESUM ranges
- No active Workers
- No active Coordinator
- Worker communication failures
- Invalid Worker startup arguments
- Duplicate Worker IDs
- RMI communication errors

The Client GUI displays error messages when invalid input is provided.

---

# Test Cases

## Test Case 1 - MAX Manual Input

Input:

```text
12,45,7,99,23,56
```

Expected result:

```text
99
```

---

## Test Case 2 - PRIMESUM Manual Input

Input:

```text
Start = 1
End = 10
```

Expected result:

```text
17
```

---

## Test Case 3 - PRIMECOUNT Manual Input

Input:

```text
2,4,5,7,9,11,12
```

Expected result:

```text
4
```

---

## Test Case 4 - CSV Input

CSV:

```text
15,8,70,22,101,4
```

Job:

```text
MAX
```

Expected result:

```text
101
```

---

## Test Case 5 - Invalid Input

Input:

```text
12, hello, 50
```

Expected behaviour:

```text
The Client displays an input error.
```

---

## Test Case 6 - Multiple Clients

Run two separate `ClientGUI` processes.

Submit jobs from both Clients.

Expected behaviour:

```text
Both Clients successfully receive their results.
```

---

## Test Case 7 - Concurrent Tasks

Submit several jobs from the Client without closing the GUI.

Expected behaviour:

```text
The Client remains responsive and the jobs complete successfully.
```

---

## Test Case 8 - Coordinator Term

Submit five complete jobs.

Expected behaviour:

```text
The fifth job completes and a new leader election begins.
```

---

# Project Structure

```text
CS324
│
├── README.md
│
└── src
    └── cs324
        │
        ├── bootstrap
        │   ├── BootstrapNode.java
        │   ├── BootstrapService.java
        │   └── BootstrapTest.java
        │
        ├── client
        │   ├── ClientConnection.java
        │   ├── ClientGUI.java
        │   └── CSVLoader.java
        │
        ├── common
        │   ├── ElectionCandidate.java
        │   ├── JobRequest.java
        │   ├── JobResult.java
        │   ├── JobType.java
        │   └── WorkerInfo.java
        │
        ├── job
        │   ├── JobCalculator.java
        │   └── JobDistributor.java
        │
        └── worker
            ├── CoordinatorTermTest.java
            ├── ElectionTest.java
            ├── JobProcessingTest.java
            ├── WorkerNode.java
            └── WorkerService.java
```

---

# Technologies Used

The project uses:

- Java
- Java RMI
- Java Swing
- Java Multithreading
- Concurrent Collections
- IntelliJ IDEA
- Git
- GitHub

---

# System Startup Summary

Start the system in this order:

```text
1. BootstrapNode
2. Worker 1 - arguments: 1 2001
3. Worker 2 - arguments: 2 2002
4. Worker 3 - arguments: 3 2003
5. ElectionTest
6. ClientGUI
```

Additional `ClientGUI` processes can be started to test multiple Clients.

---

# Authors

CS324 Distributed Systems Assignment  
University of the South Pacific