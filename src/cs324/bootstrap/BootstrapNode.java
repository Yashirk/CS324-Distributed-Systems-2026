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


public class BootstrapNode
        extends UnicastRemoteObject
        implements BootstrapService {


    // =====================================================
    // BOOTSTRAP SETTINGS
    // =====================================================

    private static final int BOOTSTRAP_PORT = 1099;

    private static final String SERVICE_NAME =
            "BootstrapService";


    // =====================================================
    // ACTIVE WORKERS
    // =====================================================

    /*
     * Stores all currently registered Workers.
     *
     * Key   = Worker ID
     * Value = Worker information
     */
    private final Map<Integer, WorkerInfo> activeWorkers;


    // =====================================================
    // CONSTRUCTOR
    // =====================================================

    public BootstrapNode()
            throws RemoteException {

        super();

        activeWorkers =
                new ConcurrentHashMap<>();
    }


    // =====================================================
    // REGISTER WORKER
    // =====================================================

    /*
     * A Worker calls this method when it starts.
     *
     * Before adding the new Worker, the Bootstrap Node
     * selects one existing Worker randomly.
     *
     * The new Worker can then use that returned Worker
     * as its first neighbour.
     */
    @Override
    public synchronized WorkerInfo registerWorker(
            WorkerInfo worker)
            throws RemoteException {


        if (worker == null) {

            throw new RemoteException(
                    "Worker information cannot be null."
            );
        }


        int workerId =
                worker.getWorkerId();


        /*
         * Worker IDs must be unique.
         */
        if (activeWorkers.containsKey(workerId)) {

            throw new RemoteException(
                    "Worker ID "
                            + workerId
                            + " is already registered."
            );
        }


        /*
         * Find a random Worker BEFORE adding
         * the new Worker.
         */
        WorkerInfo randomWorker =
                getRandomActiveWorker(
                        workerId
                );


        /*
         * Add the new Worker.
         */
        activeWorkers.put(
                workerId,
                worker
        );


        System.out.println(
                "Worker "
                        + workerId
                        + " registered."
        );


        System.out.println(
                "Active Workers: "
                        + activeWorkers.size()
        );


        /*
         * If randomWorker is null, this means
         * this is the first Worker in the network.
         */
        if (randomWorker == null) {

            System.out.println(
                    "Worker "
                            + workerId
                            + " is the first active Worker."
            );

        } else {

            System.out.println(
                    "Worker "
                            + workerId
                            + " can connect to Worker "
                            + randomWorker.getWorkerId()
            );
        }


        return randomWorker;
    }


    // =====================================================
    // UNREGISTER WORKER
    // =====================================================

    /*
     * Removes a Worker when it shuts down.
     */
    @Override
    public synchronized void unregisterWorker(
            int workerId)
            throws RemoteException {


        WorkerInfo removed =
                activeWorkers.remove(
                        workerId
                );


        if (removed != null) {

            System.out.println(
                    "Worker "
                            + workerId
                            + " unregistered."
            );

        } else {

            System.out.println(
                    "Worker "
                            + workerId
                            + " was not registered."
            );
        }


        System.out.println(
                "Active Workers: "
                        + activeWorkers.size()
        );
    }


    // =====================================================
    // GET ACTIVE WORKERS
    // =====================================================

    /*
     * Returns all currently active Workers.
     *
     * The list is sorted by Worker ID to make
     * the output predictable and easier to test.
     */
    @Override
    public List<WorkerInfo> getActiveWorkers()
            throws RemoteException {


        List<WorkerInfo> workers =
                new ArrayList<>(
                        activeWorkers.values()
                );


        workers.sort(
                Comparator.comparingInt(
                        WorkerInfo::getWorkerId
                )
        );


        return workers;
    }


    // =====================================================
    // GET RANDOM ACTIVE WORKER
    // =====================================================

    /*
     * Returns one randomly selected active Worker.
     *
     * excludeWorkerId prevents a Worker from
     * selecting itself.
     */
    @Override
    public WorkerInfo getRandomActiveWorker(
            int excludeWorkerId)
            throws RemoteException {


        List<WorkerInfo> availableWorkers =
                new ArrayList<>();


        for (WorkerInfo worker
                : activeWorkers.values()) {


            if (worker.getWorkerId()
                    != excludeWorkerId) {

                availableWorkers.add(
                        worker
                );
            }
        }


        /*
         * No other Worker exists.
         */
        if (availableWorkers.isEmpty()) {

            return null;
        }


        int randomIndex =
                ThreadLocalRandom
                        .current()
                        .nextInt(
                                availableWorkers.size()
                        );


        return availableWorkers.get(
                randomIndex
        );
    }


    // =====================================================
    // MAIN METHOD
    // =====================================================

    public static void main(
            String[] args) {


        try {

            /*
             * Create the Bootstrap Node object.
             */
            BootstrapNode bootstrap =
                    new BootstrapNode();


            /*
             * Create an RMI Registry on port 1099.
             */
            Registry registry =
                    LocateRegistry.createRegistry(
                            BOOTSTRAP_PORT
                    );


            /*
             * Register the Bootstrap service
             * inside the RMI Registry.
             */
            registry.rebind(
                    SERVICE_NAME,
                    bootstrap
            );


            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "          CS324 BOOTSTRAP NODE"
            );

            System.out.println(
                    "========================================"
            );


            System.out.println(
                    "Bootstrap Node is running."
            );


            System.out.println(
                    "RMI Registry Port: "
                            + BOOTSTRAP_PORT
            );


            System.out.println(
                    "Service Name: "
                            + SERVICE_NAME
            );


            System.out.println(
                    "Waiting for Worker registrations..."
            );


            System.out.println(
                    "========================================"
            );


        } catch (RemoteException e) {

            System.err.println(
                    "Failed to start Bootstrap Node."
            );

            e.printStackTrace();
        }
    }
}