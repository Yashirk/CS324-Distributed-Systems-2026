
package cs324.bootstrap;

import cs324.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class BootstrapGrpcServer {

    private static final int PORT = 50051;

    private static class BootstrapHandler
            extends BootstrapServiceGrpc.BootstrapServiceImplBase {

        private final Map<Integer, WorkerDetails> workers =
                new HashMap<>();

        private final Set<Integer> readyWorkers =
                new HashSet<>();

        private final int requiredWorkers;

        public BootstrapHandler(int requiredWorkers) {
            this.requiredWorkers = requiredWorkers;
        }

        @Override
        public synchronized void registerWorker(
                RegisterRequest request,
                StreamObserver<RegisterResponse> response) {

            if (!request.hasWorker()) {
                response.onError(Status.INVALID_ARGUMENT
                        .withDescription("Worker information is missing.")
                        .asRuntimeException());
                return;
            }

            WorkerDetails worker = request.getWorker();
            int workerId = worker.getWorkerId();

            if (workerId < 1 || workerId > requiredWorkers) {
                response.onError(Status.INVALID_ARGUMENT
                        .withDescription(
                                "Only Worker IDs 1 to "
                                        + requiredWorkers + " are allowed."
                        ).asRuntimeException());
                return;
            }

            if (workers.containsKey(workerId)) {
                response.onError(Status.ALREADY_EXISTS
                        .withDescription("Worker ID already registered.")
                        .asRuntimeException());
                return;
            }

            if (worker.getHost().isBlank()
                    || worker.getPort() < 1
                    || worker.getPort() > 65535) {

                response.onError(Status.INVALID_ARGUMENT
                        .withDescription("Invalid worker host or port.")
                        .asRuntimeException());
                return;
            }

            for (WorkerDetails existing : workers.values()) {
                if (existing.getHost().equalsIgnoreCase(worker.getHost())
                        && existing.getPort() == worker.getPort()) {

                    response.onError(Status.ALREADY_EXISTS
                            .withDescription("Worker port already registered.")
                            .asRuntimeException());
                    return;
                }
            }

            if (workers.size() >= requiredWorkers) {
                response.onError(Status.RESOURCE_EXHAUSTED
                        .withDescription("Worker limit reached.")
                        .asRuntimeException());
                return;
            }

            WorkerDetails neighbour = findRandomWorker(workerId);

            workers.put(workerId, worker);

            System.out.println(
                    "Worker " + workerId + " registered ("
                            + workers.size() + "/" + requiredWorkers + ")"
            );

            RegisterResponse.Builder reply =
                    RegisterResponse.newBuilder();

            if (neighbour != null) {
                reply.setNeighbour(neighbour);
            }

            response.onNext(reply.build());
            response.onCompleted();
        }

        @Override
        public synchronized void unregisterWorker(
                UnregisterRequest request,
                StreamObserver<Empty> response) {

            int workerId = request.getWorkerId();

            workers.remove(workerId);
            readyWorkers.remove(workerId);

            System.out.println(
                    "Worker " + workerId + " unregistered."
            );

            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
        }

        @Override
        public synchronized void getActiveWorkers(
                Empty request,
                StreamObserver<WorkerList> response) {

            List<WorkerDetails> list =
                    new ArrayList<>(workers.values());

            list.sort(
                    Comparator.comparingInt(WorkerDetails::getWorkerId)
            );

            WorkerList result = WorkerList.newBuilder()
                    .addAllWorkers(list)
                    .build();

            response.onNext(result);
            response.onCompleted();
        }

        @Override
        public synchronized void getRandomActiveWorker(
                RandomWorkerRequest request,
                StreamObserver<RandomWorkerResponse> response) {

            WorkerDetails worker =
                    findRandomWorker(request.getExcludeWorkerId());

            RandomWorkerResponse.Builder reply =
                    RandomWorkerResponse.newBuilder();

            if (worker != null) {
                reply.setWorker(worker);
            }

            response.onNext(reply.build());
            response.onCompleted();
        }

        @Override
        public synchronized void workerReady(
                ReadyRequest request,
                StreamObserver<ReadyResponse> response) {

            int workerId = request.getWorkerId();

            if (!workers.containsKey(workerId)) {
                response.onError(Status.NOT_FOUND
                        .withDescription("Worker is not registered.")
                        .asRuntimeException());
                return;
            }

            if (readyWorkers.add(workerId)) {
                System.out.println(
                        "Worker " + workerId + " ready ("
                                + readyWorkers.size()
                                + "/" + requiredWorkers + ")"
                );
            }

            response.onNext(
                    ReadyResponse.newBuilder()
                            .setSuccess(true)
                            .build()
            );
            response.onCompleted();

            if (workers.size() == requiredWorkers
                    && readyWorkers.size() == requiredWorkers) {

                System.out.println(
                        "All " + requiredWorkers + " workers are ready."
                );
            }
        }

        @Override
        public synchronized void isElectionReady(
                Empty request,
                StreamObserver<ElectionStatus> response) {

            boolean ready =
                    workers.size() == requiredWorkers
                            && readyWorkers.size() == requiredWorkers;

            response.onNext(
                    ElectionStatus.newBuilder()
                            .setReady(ready)
                            .build()
            );

            response.onCompleted();
        }

        private WorkerDetails findRandomWorker(int excludeId) {

            List<WorkerDetails> available = new ArrayList<>();

            for (WorkerDetails worker : workers.values()) {
                if (worker.getWorkerId() != excludeId) {
                    available.add(worker);
                }
            }

            if (available.isEmpty()) {
                return null;
            }

            int index = ThreadLocalRandom.current()
                    .nextInt(available.size());

            return available.get(index);
        }
    }

    public static void main(String[] args) throws Exception {

        Scanner scanner = new Scanner(System.in);
        int workerCount;

        while (true) {

            System.out.print(
                    "Enter number of workers (3-10): "
            );

            if (!scanner.hasNextLine()) {
                return;
            }

            String input = scanner.nextLine().trim();

            if (input.matches("-[0-9]+")) {
                System.out.println(
                        "Error: Negative numbers are not allowed."
                );
                continue;
            }

            if (!input.matches("[0-9]+")) {
                System.out.println(
                        "Error: Enter a valid whole number."
                );
                continue;
            }

            try {
                workerCount = Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.println(
                        "Error: Enter a number between 3 and 10."
                );
                continue;
            }

            if (workerCount < 3 || workerCount > 10) {
                System.out.println(
                        "Error: Worker count must be between 3 and 10."
                );
                continue;
            }

            break;
        }

        Server server = ServerBuilder.forPort(PORT)
                .addService(new BootstrapHandler(workerCount))
                .build()
                .start();

        System.out.println();
        System.out.println("========================================");
        System.out.println("       DISTRILAB GRPC BOOTSTRAP");
        System.out.println("========================================");
        System.out.println("gRPC Bootstrap Server is running.");
        System.out.println("Port: " + PORT);
        System.out.println("Required Workers: " + workerCount);
        System.out.println("Waiting for gRPC worker registrations...");
        System.out.println("========================================");

        Runtime.getRuntime().addShutdownHook(
                new Thread(server::shutdown)
        );

        server.awaitTermination();
    }
}
