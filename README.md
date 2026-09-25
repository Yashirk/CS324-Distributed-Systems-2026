# CS324 Distributed Computing System

## Overview

This project implements a distributed computing system using Java RMI.

The system contains:

- Bootstrap Node
- Multiple Worker Nodes
- Leader Election
- Job Allocation Counter (JAC)
- Distributed Job Processing
- Multithreading
- Client GUI
- CSV File Input
- Multiple Concurrent Clients

The supported computational jobs are:

- MAX(numbers)
- PRIMESUM(start, end)
- PRIMECOUNT(numbers)

---

## Requirements

To run the system you need:

- Java JDK
- IntelliJ IDEA
- Git

---

## Project Components

### Bootstrap Node

The Bootstrap Node keeps track of currently active Worker Nodes.

It runs separately and does not participate in leader election or job processing.

### Worker Nodes

Each Worker Node:

- Has a unique Worker ID
- Runs as a separate Java process
- Registers with the Bootstrap Node
- Connects to another active worker
- Communicates using Java RMI
- Participates in leader elections
- Processes distributed jobs

### Coordinator

One Worker acts as the Coordinator.

The Coordinator:

- Receives jobs from clients
- Splits jobs between available workers
- Collects partial results
- Returns the final result to the client

After five jobs, a new leader election is started.

### Client GUI

The Client GUI allows the user to:

- Select a job type
- Enter data manually
- Load data from a CSV file
- Submit jobs
- View computation results
- View the Coordinator
- View task activity

Multiple Client GUI processes can run at the same time.

---

# How to Run the System

The system should be started in the following order.

## Step 1 - Start Bootstrap Node

Run:

```text
BootstrapNode