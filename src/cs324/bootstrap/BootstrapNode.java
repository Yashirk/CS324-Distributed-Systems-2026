package cs324.bootstrap;

import cs324.common.WorkerInfo;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class BootstrapNode extends UnicastRemoteObject implements BootstrapService {

    private static final int BOOTSTRAP_PORT = 1099;
    private static final String SERVICE_NAME = "BootstrapService";

    private final Map<Integer, WorkerInfo> activeWorkers;

    public BootstrapNode() throws RemoteException {
        super();
        activeWorkers = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized WorkerInfo registerWorker(WorkerInfo worker)
            throws RemoteException {

        if (worker == null) {
            throw new RemoteException("Worker information cannot be null.");
        }

        if (activeWorkers.containsKey(worker.getWorkerId())) {
            throw new RemoteException(
                    "Worker ID " + worker.getWorkerId() + " is already registered."
            );
        }

        WorkerInfo randomNeighbour =
                getRandomActiveWorker(worker.getWorkerId());

        activeWorkers.put(worker.getWorkerId(), worker);

        System.out.println("Worker registered: " + worker);
        System.out.println("Total active workers: " + activeWorkers.size());

        if (randomNeighbour == null) {
            System.out.println("No existing worker available as neighbour.");
        } else {
            System.out.println(
                    "Suggested neighbour for Worker "
                            + worker.getWorkerId()
                            + ": Worker "
                            + randomNeighbour.getWorkerId()
            );
        }

        return randomNeighbour;
    }

    @Override
    public synchronized void unregisterWorker(int workerId)
            throws RemoteException {

        WorkerInfo removed = activeWorkers.remove(workerId);

        if (removed != null) {
            System.out.println("Worker " + workerId + " unregistered.");
        } else {
            System.out.println("Worker " + workerId + " was not registered.");
        }
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

        List<WorkerInfo> candidates = new ArrayList<>();

        for (WorkerInfo worker : activeWorkers.values()) {

            if (worker.getWorkerId() != excludeWorkerId) {
                candidates.add(worker);
            }
        }

        if (candidates.isEmpty()) {
            return null;
        }

        int randomIndex =
                ThreadLocalRandom.current()
                        .nextInt(candidates.size());

        return candidates.get(randomIndex);
    }

    public static void main(String[] args) {

        try {

            BootstrapNode bootstrap =
                    new BootstrapNode();

            Registry registry =
                    LocateRegistry.createRegistry(
                            BOOTSTRAP_PORT
                    );

            registry.rebind(
                    SERVICE_NAME,
                    bootstrap
            );

            System.out.println("======================================");
            System.out.println("CS324 BOOTSTRAP NODE");
            System.out.println("======================================");
            System.out.println("Bootstrap Node is running.");
            System.out.println("RMI Port: " + BOOTSTRAP_PORT);
            System.out.println("Service Name: " + SERVICE_NAME);
            System.out.println("Waiting for workers to register...");
            System.out.println("======================================");

        } catch (Exception e) {

            System.err.println("Bootstrap Node failed to start.");
            e.printStackTrace();
        }
    }
}