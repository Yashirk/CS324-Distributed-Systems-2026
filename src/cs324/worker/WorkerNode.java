package cs324.worker;

import cs324.bootstrap.BootstrapService;
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
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerNode extends UnicastRemoteObject
        implements WorkerService {

    // Unique information for this worker
    private final int workerId;
    private final String host;
    private final int registryPort;
    private final String serviceName;

    // Required by the assignment
    private final String leaderman = "cs324";

    // Job Allocation Counter
    private final AtomicInteger jac = new AtomicInteger(0);

    // Stores this worker's neighbours
    private final Map<Integer, WorkerInfo> neighbours =
            new ConcurrentHashMap<>();


    // Constructor
    public WorkerNode(
            int workerId,
            String host,
            int registryPort,
            String serviceName)
            throws RemoteException {

        super();

        this.workerId = workerId;
        this.host = host;
        this.registryPort = registryPort;
        this.serviceName = serviceName;
    }


    // Return Worker ID
    @Override
    public int getWorkerId() throws RemoteException {

        return workerId;
    }


    // Return current JAC
    @Override
    public int getJAC() throws RemoteException {

        return jac.get();
    }


    // Increase JAC
    @Override
    public void incrementJAC() throws RemoteException {

        jac.incrementAndGet();

        System.out.println(
                "Worker " + workerId +
                        " JAC increased to " + jac.get()
        );
    }


    // Return required leaderman value
    @Override
    public String getLeaderman() throws RemoteException {

        return leaderman;
    }


    // Add another worker as a neighbour
    @Override
    public void addNeighbour(WorkerInfo worker)
            throws RemoteException {

        if (worker == null) {

            return;
        }

        // Worker should not add itself as neighbour
        if (worker.getWorkerId() == workerId) {

            return;
        }

        neighbours.put(
                worker.getWorkerId(),
                worker
        );

        System.out.println(
                "Worker " + workerId +
                        " added Worker " +
                        worker.getWorkerId() +
                        " as a neighbour."
        );
    }


    // Return neighbour list
    @Override
    public List<WorkerInfo> getNeighbours()
            throws RemoteException {

        List<WorkerInfo> neighbourList =
                new ArrayList<>(
                        neighbours.values()
                );

        neighbourList.sort(
                Comparator.comparingInt(
                        WorkerInfo::getWorkerId
                )
        );

        return neighbourList;
    }


    // Connect to a neighbour supplied by Bootstrap
    private void connectToNeighbour(
            WorkerInfo neighbourInfo) {

        if (neighbourInfo == null) {

            System.out.println(
                    "No neighbour available."
            );

            System.out.println(
                    "This is probably the first worker."
            );

            return;
        }

        try {

            // Store neighbour locally
            neighbours.put(
                    neighbourInfo.getWorkerId(),
                    neighbourInfo
            );

            // Connect to neighbour's RMI Registry
            Registry neighbourRegistry =
                    LocateRegistry.getRegistry(
                            neighbourInfo.getHost(),
                            neighbourInfo.getRegistryPort()
                    );

            // Find neighbour's WorkerService
            WorkerService neighbour =
                    (WorkerService)
                            neighbourRegistry.lookup(
                                    neighbourInfo.getServiceName()
                            );

            // Information about this worker
            WorkerInfo myInfo =
                    new WorkerInfo(
                            workerId,
                            host,
                            registryPort,
                            serviceName
                    );

            // Tell the existing worker about this new worker
            neighbour.addNeighbour(myInfo);

            System.out.println(
                    "Worker " + workerId +
                            " successfully connected to Worker " +
                            neighbourInfo.getWorkerId()
            );

        } catch (Exception e) {

            System.err.println(
                    "Worker " + workerId +
                            " could not connect to neighbour Worker " +
                            neighbourInfo.getWorkerId()
            );

            e.printStackTrace();
        }
    }


    // Display Worker details
    private void printWorkerDetails() {

        System.out.println(
                "======================================"
        );

        System.out.println(
                "CS324 WORKER NODE"
        );

        System.out.println(
                "======================================"
        );

        System.out.println(
                "Worker ID: " + workerId
        );

        System.out.println(
                "Host: " + host
        );

        System.out.println(
                "RMI Port: " + registryPort
        );

        System.out.println(
                "Service Name: " + serviceName
        );

        System.out.println(
                "JAC: " + jac.get()
        );

        System.out.println(
                "leaderman: " + leaderman
        );

        System.out.println(
                "======================================"
        );
    }


    // MAIN METHOD
    public static void main(String[] args) {

        // We need Worker ID and Port
        if (args.length < 2) {

            System.out.println(
                    "Usage: WorkerNode <workerId> <port>"
            );

            System.out.println(
                    "Example:"
            );

            System.out.println(
                    "WorkerNode 1 2001"
            );

            return;
        }

        try {

            // Read Worker ID
            int workerId =
                    Integer.parseInt(
                            args[0]
                    );

            // Read Worker Port
            int workerPort =
                    Integer.parseInt(
                            args[1]
                    );

            // For testing on same laptop
            String workerHost =
                    "localhost";

            // Example:
            // Worker1
            // Worker2
            // Worker3
            String serviceName =
                    "Worker" + workerId;


            // Create Worker object
            WorkerNode worker =
                    new WorkerNode(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            // Start this Worker's own RMI Registry
            Registry workerRegistry =
                    LocateRegistry.createRegistry(
                            workerPort
                    );


            // Put Worker into RMI Registry
            workerRegistry.rebind(
                    serviceName,
                    worker
            );


            // Print Worker details
            worker.printWorkerDetails();


            /*
             * CONNECT TO BOOTSTRAP
             */

            Registry bootstrapRegistry =
                    LocateRegistry.getRegistry(
                            "localhost",
                            1099
                    );


            BootstrapService bootstrap =
                    (BootstrapService)
                            bootstrapRegistry.lookup(
                                    "BootstrapService"
                            );


            // Create Worker information
            WorkerInfo myInfo =
                    new WorkerInfo(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            /*
             * Register this Worker with Bootstrap.
             *
             * Bootstrap will return one random
             * existing Worker to use as a neighbour.
             */
            WorkerInfo neighbour =
                    bootstrap.registerWorker(
                            myInfo
                    );


            System.out.println(
                    "Worker " + workerId +
                            " registered with Bootstrap successfully."
            );


            /*
             * Connect to random neighbour
             */
            worker.connectToNeighbour(
                    neighbour
            );


            System.out.println(
                    "Worker " + workerId +
                            " is now ready."
            );


            /*
             * When Worker closes,
             * remove it from Bootstrap.
             */
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                try {

                                    bootstrap.unregisterWorker(
                                            workerId
                                    );

                                    System.out.println(
                                            "Worker " +
                                                    workerId +
                                                    " removed from Bootstrap."
                                    );

                                } catch (Exception ignored) {

                                }
                            })
                    );


        } catch (NumberFormatException e) {

            System.err.println(
                    "Worker ID and Port must be numbers."
            );

        } catch (Exception e) {

            System.err.println(
                    "Worker failed to start."
            );

            e.printStackTrace();
        }
    }
}