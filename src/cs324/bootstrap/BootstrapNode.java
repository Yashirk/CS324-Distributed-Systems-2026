
package cs324.bootstrap;

import cs324.common.WorkerInfo;
import cs324.worker.WorkerService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class BootstrapNode extends UnicastRemoteObject
        implements BootstrapService {

    private static final int BOOTSTRAP_PORT = 1099;
    private static final String SERVICE_NAME = "BootstrapService";

    private final Map<Integer, WorkerInfo> activeWorkers;
    private final Set<Integer> readyWorkers;
    private final int requiredWorkers;

    public BootstrapNode(int requiredWorkers) throws RemoteException {
        super();

        this.requiredWorkers = requiredWorkers;
        activeWorkers = new ConcurrentHashMap<>();
        readyWorkers = new HashSet<>();
    }

    public BootstrapNode() throws RemoteException {
        this(10);
    }

    @Override
    public synchronized WorkerInfo registerWorker(WorkerInfo worker)
            throws RemoteException {

        if (worker == null) {
            throw new RemoteException("Worker information is missing.");
        }

        int workerId = worker.getWorkerId();

        // Only accept the worker IDs selected at startup
        if (workerId < 1 || workerId > requiredWorkers) {
            throw new RemoteException(
                    "Worker " + workerId + " rejected. "
                            + "Allowed Worker IDs are 1 to "
                            + requiredWorkers + "."
            );
        }

        if (activeWorkers.containsKey(workerId)) {
            throw new RemoteException(
                    "Worker " + workerId + " is already registered."
            );
        }

        if (activeWorkers.size() >= requiredWorkers) {
            throw new RemoteException(
                    "Worker limit reached. Only "
                            + requiredWorkers + " workers are allowed."
            );
        }

        // Workers on the same host must use different ports
        for (WorkerInfo existing : activeWorkers.values()) {
            if (existing.getHost().equalsIgnoreCase(worker.getHost())
                    && existing.getRegistryPort() == worker.getRegistryPort()) {

                throw new RemoteException(
                        "Port " + worker.getRegistryPort()
                                + " is already registered by Worker "
                                + existing.getWorkerId() + "."
                );
            }
        }

        WorkerInfo neighbour = getRandomActiveWorker(workerId);
        activeWorkers.put(workerId, worker);

        System.out.println(
                "Worker " + workerId + " registered ("
                        + activeWorkers.size() + "/" + requiredWorkers + ")."
        );

        if (neighbour == null) {
            System.out.println(
                    "Worker " + workerId + " is the first worker."
            );
        } else {
            System.out.println(
                    "Worker " + workerId
                            + " can connect to Worker "
                            + neighbour.getWorkerId()
            );
        }

        return neighbour;
    }

    @Override
    public synchronized void workerReady(int workerId)
            throws RemoteException {

        if (!activeWorkers.containsKey(workerId)) {
            throw new RemoteException(
                    "Worker " + workerId + " is not registered."
            );
        }

        if (readyWorkers.add(workerId)) {
            System.out.println(
                    "Worker " + workerId + " is ready ("
                            + readyWorkers.size() + "/"
                            + requiredWorkers + ")."
            );
        }

        notifyAll();
    }

    @Override
    public synchronized boolean isElectionReady()
            throws RemoteException {

        return activeWorkers.size() == requiredWorkers
                && readyWorkers.size() == requiredWorkers;
    }

    @Override
    public synchronized void unregisterWorker(int workerId)
            throws RemoteException {

        WorkerInfo removed = activeWorkers.remove(workerId);
        readyWorkers.remove(workerId);

        if (removed != null) {
            System.out.println(
                    "Worker " + workerId + " unregistered."
            );
        }

        System.out.println(
                "Active Workers: " + activeWorkers.size()
        );
    }

    @Override
    public List<WorkerInfo> getActiveWorkers()
            throws RemoteException {

        List<WorkerInfo> workers =
                new ArrayList<>(activeWorkers.values());

        workers.sort(
                Comparator.comparingInt(WorkerInfo::getWorkerId)
        );

        return workers;
    }

    @Override
    public WorkerInfo getRandomActiveWorker(int excludeWorkerId)
            throws RemoteException {

        List<WorkerInfo> available = new ArrayList<>();

        for (WorkerInfo worker : activeWorkers.values()) {
            if (worker.getWorkerId() != excludeWorkerId) {
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

    private synchronized void waitForWorkers()
            throws InterruptedException {

        while (activeWorkers.size() != requiredWorkers
                || readyWorkers.size() != requiredWorkers) {

            wait();
        }
    }

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        int workerCount;

        while (true) {

            System.out.print("Enter number of workers (3-10): ");
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
                        "Error: Please choose between 3 and 10 workers."
                );
                continue;
            }

            break;
        }

        try {

            BootstrapNode bootstrap = new BootstrapNode(workerCount);

            Registry registry =
                    LocateRegistry.createRegistry(BOOTSTRAP_PORT);

            registry.rebind(SERVICE_NAME, bootstrap);

            System.out.println();
            System.out.println("========================================");
            System.out.println("          CS324 BOOTSTRAP NODE");
            System.out.println("========================================");
            System.out.println("Bootstrap Node is running.");
            System.out.println("Required Workers: " + workerCount);
            System.out.println("RMI Port: " + BOOTSTRAP_PORT);
            System.out.println("========================================");

            System.out.println(
                    "Waiting for Workers 1 to " + workerCount + "..."
            );

            bootstrap.waitForWorkers();

            System.out.println();
            System.out.println(
                    "All " + workerCount + " workers are ready."
            );
            System.out.println("Starting coordinator election...");

            List<WorkerInfo> workers = bootstrap.getActiveWorkers();
            WorkerInfo firstWorker = workers.get(0);

            Registry workerRegistry = LocateRegistry.getRegistry(
                    firstWorker.getHost(),
                    firstWorker.getRegistryPort()
            );

            WorkerService worker = (WorkerService)
                    workerRegistry.lookup(firstWorker.getServiceName());


            worker.startElection();

            System.out.println();
            System.out.println("========================================");
            System.out.println("       COORDINATOR ELECTION RESULT");
            System.out.println("========================================");

            int electedCoordinator = -1;
            boolean allAgree = true;

            for (WorkerInfo info : workers) {

                Registry remoteRegistry = LocateRegistry.getRegistry(
                        info.getHost(),
                        info.getRegistryPort()
                );

                WorkerService remoteWorker = (WorkerService)
                        remoteRegistry.lookup(info.getServiceName());

                int coordinator = remoteWorker.getCoordinatorId();

                System.out.println(
                        "Worker " + info.getWorkerId()
                                + " says Coordinator = Worker "
                                + coordinator
                );

                if (coordinator == -1) {
                    allAgree = false;
                } else if (electedCoordinator == -1) {
                    electedCoordinator = coordinator;
                } else if (coordinator != electedCoordinator) {
                    allAgree = false;
                }
            }

            System.out.println("========================================");

            if (allAgree) {
                System.out.println(
                        "ELECTED COORDINATOR: Worker " + electedCoordinator
                );
                System.out.println("LEADER ELECTION SUCCESSFUL.");
            } else {
                System.out.println("LEADER ELECTION FAILED.");
            }


        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            System.err.println("Bootstrap waiting was interrupted.");

        } catch (Exception e) {

            System.err.println(
                    "Bootstrap Node or election failed."
            );
            e.printStackTrace();
        }
    }
}
