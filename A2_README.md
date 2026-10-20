# CS324 DistriLab Assignment 2 – gRPC version

This branch upgrades the A1 distributed cluster to gRPC and Protocol Buffers. The original Java RMI source is retained for reference under `archive/A1-java-rmi` **outside the Maven source tree**. The A2 bootstrap, workers, GUI, and client communicate using gRPC only.

## Building in IntelliJ (Windows)

1. Open `CS324/pom.xml` as a Maven project in IntelliJ. Wait for Maven to download dependencies.
2. In the Maven tool window, run **Lifecycle → compile** (or `mvn clean compile` if Maven is installed in your terminal).
3. Maven generates the Java protobuf/gRPC classes automatically. Do not edit files under `target/`.
4. Stop old A1/RMI Java processes before launching the new system. Port `50051` is Bootstrap; workers use `2001` through `2010`.

## Run order

1. Run `cs324.bootstrap.BootstrapGrpcServer`. Enter an integer between 3 and 10.
2. Make ten **Application** run configurations for main class `cs324.worker.WorkerGrpcNode` with program arguments `1 2001`, `2 2002`, ... `10 2010`. Run only the requested IDs, for example 1–3 for three workers. Keep every process running.
3. Bootstrap waits until all requested workers have registered and sent `WorkerReady`. Nodes then elect a coordinator: **lowest JAC wins, highest ID breaks ties**. The elected coordinator appears in the worker terminals.
4. Run `cs324.client.ClientGUI`, choose MAX, PRIMESUM or PRIMECOUNT. Enter integer values manually or load numeric CSV data. Click **Submit Job**.
5. Click the progress bar beside a job in **Task Activity** for its actual worker-level completed and assigned task counts.
6. Optional: run `cs324.a2.A2SmokeTest` for MAX with huge numbers, PRIMESUM 2..10 and PRIMECOUNT with six integers.

## Demonstrating failure tolerance

- **Worker failure:** While a sufficiently long job runs, stop a non-coordinator worker from IntelliJ. The coordinator catches the broken RPC and retries unfinished chunks on other available workers. Bootstrap also detects missing worker heartbeats after approximately six seconds. Do not expect already completed task percentages to reset.
- **Worker rejoins:** Start a replacement with the original worker ID and port after Bootstrap has removed the old registration. The replacement syncs past jobs from a neighbour and can receive new tasks. A worker ID exceeding the originally entered limit is rejected.
- **Coordinator failure:** While processing, stop the coordinator process. Bootstrap marks it offline after about six seconds; surviving workers elect a replacement using JAC and ID. Completed task results are replicated to the other workers, and the replacement resumes missing chunks. The GUI retries its original job ID on the new coordinator.
- **Five completed jobs:** A coordinator raises its JAC and triggers another election after its fifth completed job.

## Technical notes and constraints

- `bootstrap.proto` defines worker discovery, readiness and heartbeats; `worker.proto` defines worker RPCs, task replication and server-streamed progress events.
- Progress percentage is **completed tasks divided by total tasks**, not elapsed time.
- Job IDs identify identical submissions across client reconnections. Task indices deduplicate late or repeated results.
- Task plans and completed outcomes are replicated in workers' **memory**. This allows one-node failure recovery when backups remain alive. **It is not durable recovery from all workers crashing simultaneously or a Bootstrap crash.**
- This demo assumes workers and the Bootstrap run on the same machine (`localhost`), uses unencrypted gRPC, and processes a reasonable number of input items/range steps. BigInteger avoids integer overflow but cannot make astronomically large prime ranges finish quickly. Probable primality is used for large inputs.
- The program has not been tested across separate PCs or with a full network partition.
