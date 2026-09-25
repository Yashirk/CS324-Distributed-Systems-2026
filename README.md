# CS324 Distributed Computing System

## Overview

This project implements a distributed computing system using Java RMI.

The system contains:

- Bootstrap Node
- Multiple Worker Nodes
- Unstructured Worker Network
- Leader Election
- Job Allocation Counter (JAC)
- Distributed Job Processing
- Java Multithreading
- Client GUI
- CSV File Input
- Multiple Concurrent Clients

The supported computational jobs are:

- `MAX(numbers)`
- `PRIMESUM(start, end)`
- `PRIMECOUNT(numbers)`

---

# Requirements

To run the system you need:

- Java JDK
- IntelliJ IDEA
- Git
- GitHub

---

# Project Components

## Bootstrap Node

The Bootstrap Node keeps track of all currently active Worker Nodes.

It:

- Runs as a separate Java process
- Uses Java RMI
- Registers active Workers
- Provides active Worker information
- Helps new Workers find another active Worker
- Does not participate in leader elections
- Does not perform computational jobs

---

## Worker Nodes

Each Worker Node:

- Runs as a separate Java process
- Has a unique integer Worker ID
- Has its own RMI registry port
- Registers with the Bootstrap Node
- Connects to another active Worker
- Maintains neighbouring Workers
- Participates in leader elections
- Maintains a Job Allocation Counter (JAC)
- Processes distributed jobs
- Supports concurrent job execution using Java threads

---

## Leader Election

When an election is started:

1. An `ELECTION` message is propagated through the Worker network.
2. Each Worker processes the same election only once.
3. All reachable active Workers are considered.
4. The Worker with the lowest JAC is selected.
5. If Workers have the same lowest JAC, the Worker with the highest Worker ID wins.
6. A `COORDINATOR` message is propagated through the network.
7. All Workers agree on the same Coordinator.

Each Worker also contains:

```text
leaderman = "cs324"
```

---

## Coordinator

The elected Coordinator:

- Accepts jobs from Clients
- Divides jobs between active Workers
- Sends partial jobs to Workers using Java RMI
- Collects partial results
- Combines the partial results
- Returns the final result to the Client

A Coordinator can serve for a maximum of five complete jobs.

After the fifth job, a new leader election begins.

---

## Client GUI

The Client provides a graphical user interface.

The user can:

- Select a job type
- Enter data manually
- Load data from a CSV file
- Submit jobs
- View computation results
- View the Coordinator
- View task activity
- Run multiple jobs concurrently

Multiple Client GUI processes can also run at the same time.

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

The Bootstrap Node uses port:

```text
1099
```

Expected output will indicate that the Bootstrap Node is running.

Keep this process running.

---

## Step 2 - Start Worker 1

Run `WorkerNode` with the following program arguments:

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

Run another `WorkerNode` process with:

```text
2 2002
```

This creates:

```text
Worker ID: 2
Registry Port: 2002
```

Worker 2 registers with the Bootstrap Node and connects to an active Worker.

Keep Worker 2 running.

---

## Step 4 - Start Worker 3

Run another `WorkerNode` process with:

```text
3 2003
```

This creates:

```text
Worker ID: 3
Registry Port: 2003
```

Worker 3 joins the Worker network.

Keep Worker 3 running.

---

## Step 5 - Start the Leader Election

Open:

```text
src/cs324/worker/ElectionTest.java
```

Run:

```text
ElectionTest
```

The Workers participate in the leader election.

The Worker with the lowest JAC becomes Coordinator.

If Workers have equal JAC values, the Worker with the highest Worker ID becomes Coordinator.

Example when all JAC values are initially `0`:

```text
Worker 3
```

may become Coordinator because it has the highest Worker ID.

All active Workers should eventually report the same Coordinator.

---

## Step 6 - Start the Client GUI

Open:

```text
src/cs324/client/ClientGUI.java
```

Run:

```text
ClientGUI
```

The graphical Client application will open.

The Client communicates with the elected Coordinator using Java RMI.

---

# Using the Client GUI

## MAX

Select:

```text
MAX
```

Example manual input:

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

Prime numbers between 1 and 10 are:

```text
2, 3, 5, 7
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

The prime numbers are:

```text
2, 5, 7, 11
```

Expected result:

```text
4
```

---

# Loading Data From CSV

The Client can load numerical input from a CSV file.

## MAX CSV Example

Create:

```text
numbers.csv
```

Example contents:

```text
15,8,70,22,101,4
```

In the Client:

1. Select `MAX`.
2. Click `Load CSV`.
3. Select `numbers.csv`.
4. Confirm the numbers appear in the input field.
5. Click `Submit Job`.

Expected result:

```text
101
```

---

## PRIMECOUNT CSV Example

Example CSV contents:

```text
2,3,4,5,8,13,20
```

Select:

```text
PRIMECOUNT
```

Load the file and submit the job.

Expected result:

```text
4
```

---

## PRIMESUM CSV Example

For `PRIMESUM`, the CSV should contain at least two numbers.

Example:

```text
1,10
```

The first number becomes:

```text
Start = 1
```

The second number becomes:

```text
End = 10
```

Expected result:

```text
17
```

---

# Multiple Concurrent Jobs

A single Client can submit multiple jobs.

The Client uses a Java thread pool so job submission does not freeze the graphical interface.

Example:

```text
MAX
5,20,80,3
```

Expected:

```text
80
```

Another task can also be submitted while other work is being processed.

Example:

```text
PRIMECOUNT
2,3,4,5,8,11
```

Expected:

```text
4
```

---

# Running Multiple Clients

The system supports multiple Client processes.

Run:

```text
ClientGUI
```

more than once.

For example:

```text
Client 1
MAX
12,45,7,99
```

Expected:

```text
99
```

At the same time, another Client may run:

```text
Client 2
PRIMESUM
1 to 20
```

Expected:

```text
77
```

Both Clients communicate with the Coordinator using Java RMI.

---

# Distributed Job Processing

The Coordinator divides submitted jobs as evenly as possible among active Workers.

For example:

```text
PRIMESUM(1, 1000)
```

with four Workers could be divided as:

```text
Worker 1: 1 - 250
Worker 2: 251 - 500
Worker 3: 501 - 750
Worker 4: 751 - 1000
```

Each Worker processes its assigned part.

The Coordinator then combines the partial results to produce the final result.

---

# Job Allocation Counter (JAC)

Each Worker maintains a Job Allocation Counter.

When a Worker is acting as Coordinator and assigns work to another Worker, its JAC is incremented.

The JAC is used during leader election.

The Worker with the lowest JAC is preferred as the new Coordinator.

If multiple Workers have the same lowest JAC, the Worker with the highest ID is selected.

---

# Coordinator Term

A Coordinator can process up to five complete jobs during one term.

After Job 5:

1. The current Coordinator term ends.
2. A new leader election starts.
3. Active Workers compare their JAC values.
4. A new Coordinator is selected.
5. The Coordinator result is propagated to the Worker network.

---

# Test Cases

## Test Case 1 - MAX

Input:

```text
12,45,7,99,23,56
```

Expected:

```text
99
```

---

## Test Case 2 - PRIMESUM

Input:

```text
Start = 1
End = 10
```

Expected:

```text
17
```

---

## Test Case 3 - PRIMECOUNT

Input:

```text
2,4,5,7,9,11,12
```

Expected:

```text
4
```

---

## Test Case 4 - CSV

CSV:

```text
15,8,70,22,101,4
```

Job:

```text
MAX
```

Expected:

```text
101
```

---

## Test Case 5 - Invalid Input

Example:

```text
12, hello, 50
```

Expected behaviour:

```text
The Client displays an input error instead of submitting an invalid job.
```

---

## Test Case 6 - Multiple Clients

Run two separate `ClientGUI` processes.

Submit a job from both Clients.

Expected behaviour:

```text
Both Clients successfully receive their results.
```

---

## Test Case 7 - Coordinator Term

Submit five complete jobs.

Expected behaviour:

```text
After the fifth job, the Coordinator term ends and a new leader election occurs.
```

---

# Project Structure

```text
src
└── cs324
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

- Java
- Java RMI
- Java Swing
- Java Multithreading
- IntelliJ IDEA
- Git
- GitHub

---

# System Startup Summary

Always start the system in this order:

```text
1. BootstrapNode
2. Worker 1 - arguments: 1 2001
3. Worker 2 - arguments: 2 2002
4. Worker 3 - arguments: 3 2003
5. ElectionTest
6. ClientGUI
```

Additional `ClientGUI` processes can then be started if multiple Clients are required.

---

# Authors

CS324 Distributed Systems Assignment  
University of the South Pacific