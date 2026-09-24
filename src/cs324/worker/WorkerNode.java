package cs324.worker;

import cs324.bootstrap.BootstrapService;
import cs324.common.ElectionCandidate;
import cs324.common.WorkerInfo;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerNode extends UnicastRemoteObject
        implements WorkerService {

    // ==========================================
    // BASIC WORKER INFORMATION
    // ==========================================

    private final int workerId;
    private final String host;
    private final int registryPort;
    private final String serviceName;

    // Required by assignment
    private final String leaderman = "cs324";

    // Job Allocation Counter
    private final AtomicInteger jac =
            new AtomicInteger(0);

    // Current coordinator
    // -1 means no coordinator currently known
    private final AtomicInteger coordinatorId =
            new AtomicInteger(-1);

    // Direct neighbours of this worker
    private final Map<Integer, WorkerInfo> neighbours =
            new ConcurrentHashMap<>();

    // Elections already processed by this worker
    private final Set<String> processedElections =
            ConcurrentHashMap.newKeySet();

    // Coordinator messages already processed
    private final Set<String> processedCoordinatorMessages =
            ConcurrentHashMap.newKeySet();


    // ==========================================
    // CONSTRUCTOR
    // ==========================================

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


    // ==========================================
    // BASIC WORKER METHODS
    // ==========================================

    @Override
    public int getWorkerId()
            throws RemoteException {

        return workerId;
    }


    @Override
    public int getJAC()
            throws RemoteException {

        return jac.get();
    }


    @Override
    public void incrementJAC()
            throws RemoteException {

        int newValue =
                jac.incrementAndGet();

        System.out.println(
                "Worker " + workerId +
                        " JAC increased to " +
                        newValue
        );
    }


    @Override
    public String getLeaderman()
            throws RemoteException {

        return leaderman;
    }


    @Override
    public void addNeighbour(
            WorkerInfo worker)
            throws RemoteException {

        if (worker == null) {
            return;
        }

        // Do not add yourself as neighbour
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


    // ==========================================
    // LEADER ELECTION
    // ==========================================

    @Override
    public void startElection()
            throws RemoteException {

        /*
         * Create a unique ID for this election.
         * Example:
         * c9fa84be-....
         */
        String electionId =
                UUID.randomUUID().toString();

        System.out.println();
        System.out.println(
                "======================================"
        );

        System.out.println(
                "Worker " + workerId +
                        " STARTED LEADER ELECTION"
        );

        System.out.println(
                "Election ID: " + electionId
        );

        System.out.println(
                "======================================"
        );

        /*
         * Propagate the election through
         * the whole reachable worker network.
         */
        List<ElectionCandidate> candidates =
                processElection(
                        electionId,
                        -1
                );


        if (candidates.isEmpty()) {

            System.out.println(
                    "No candidates found."
            );

            return;
        }


        System.out.println();
        System.out.println(
                "Candidates discovered:"
        );

        for (ElectionCandidate candidate
                : candidates) {

            System.out.println(
                    candidate
            );
        }


        /*
         * Choose winner.
         *
         * RULE 1:
         * Lowest JAC wins.
         *
         * RULE 2:
         * If JAC is equal,
         * highest Worker ID wins.
         */

        ElectionCandidate winner = null;

        for (ElectionCandidate candidate
                : candidates) {

            if (winner == null) {

                winner = candidate;

                continue;
            }

            // Lower JAC wins
            if (candidate.getJac()
                    < winner.getJac()) {

                winner = candidate;
            }

            // If same JAC,
            // higher ID wins
            else if (
                    candidate.getJac()
                            == winner.getJac()
                            &&
                            candidate.getWorkerId()
                                    > winner.getWorkerId()
            ) {

                winner = candidate;
            }
        }


        if (winner == null) {
            return;
        }


        System.out.println();
        System.out.println(
                "======================================"
        );

        System.out.println(
                "ELECTION WINNER"
        );

        System.out.println(
                "Worker ID: "
                        + winner.getWorkerId()
        );

        System.out.println(
                "JAC: "
                        + winner.getJac()
        );

        System.out.println(
                "======================================"
        );


        /*
         * Tell all workers who won.
         */
        announceCoordinator(
                electionId,
                winner.getWorkerId(),
                -1
        );
    }


    // ==========================================
    // PROCESS ELECTION MESSAGE
    // ==========================================

    @Override
    public List<ElectionCandidate> processElection(
            String electionId,
            int senderId)
            throws RemoteException {

        /*
         * DUPLICATE PROTECTION
         *
         * If this worker has already processed
         * this election ID, ignore it.
         */

        boolean firstTime =
                processedElections.add(
                        electionId
                );

        if (!firstTime) {

            System.out.println(
                    "Worker " + workerId +
                            " ignored duplicate election "
                            + electionId
            );

            return new ArrayList<>();
        }


        System.out.println(
                "Worker " + workerId +
                        " processing ELECTION message."
        );


        List<ElectionCandidate> candidates =
                new ArrayList<>();


        /*
         * Add this worker to election.
         */
        candidates.add(
                new ElectionCandidate(
                        workerId,
                        jac.get()
                )
        );


        /*
         * Send election to every neighbour.
         */
        for (WorkerInfo neighbourInfo
                : neighbours.values()) {

            /*
             * No need to immediately send
             * message back to sender.
             */
            if (neighbourInfo.getWorkerId()
                    == senderId) {

                continue;
            }


            try {

                WorkerService neighbour =
                        getRemoteWorker(
                                neighbourInfo
                        );


                List<ElectionCandidate>
                        neighbourCandidates =
                        neighbour.processElection(
                                electionId,
                                workerId
                        );


                candidates.addAll(
                        neighbourCandidates
                );


            } catch (Exception e) {

                System.err.println(
                        "Worker " + workerId +
                                " could not send election to Worker "
                                + neighbourInfo.getWorkerId()
                );
            }
        }


        return candidates;
    }


    // ==========================================
    // COORDINATOR ANNOUNCEMENT
    // ==========================================

    @Override
    public void announceCoordinator(
            String electionId,
            int newCoordinatorId,
            int senderId)
            throws RemoteException {

        /*
         * Prevent processing the same
         * COORDINATOR message more than once.
         */

        boolean firstTime =
                processedCoordinatorMessages.add(
                        electionId
                );


        if (!firstTime) {

            return;
        }


        /*
         * Save coordinator locally.
         */

        coordinatorId.set(
                newCoordinatorId
        );


        System.out.println();
        System.out.println(
                "Worker " + workerId +
                        " received COORDINATOR message."
        );

        System.out.println(
                "Current Coordinator = Worker "
                        + newCoordinatorId
        );


        /*
         * Propagate coordinator message
         * to neighbours.
         */

        for (WorkerInfo neighbourInfo
                : neighbours.values()) {


            if (neighbourInfo.getWorkerId()
                    == senderId) {

                continue;
            }


            try {

                WorkerService neighbour =
                        getRemoteWorker(
                                neighbourInfo
                        );


                neighbour.announceCoordinator(
                        electionId,
                        newCoordinatorId,
                        workerId
                );


            } catch (Exception e) {

                System.err.println(
                        "Worker " + workerId +
                                " could not send coordinator message to Worker "
                                + neighbourInfo.getWorkerId()
                );
            }
        }
    }


    // ==========================================
    // GET CURRENT COORDINATOR
    // ==========================================

    @Override
    public int getCoordinatorId()
            throws RemoteException {

        return coordinatorId.get();
    }


    // ==========================================
    // HELPER:
    // CONNECT TO REMOTE WORKER
    // ==========================================

    private WorkerService getRemoteWorker(
            WorkerInfo workerInfo)
            throws Exception {

        Registry registry =
                LocateRegistry.getRegistry(
                        workerInfo.getHost(),
                        workerInfo.getRegistryPort()
                );


        return (WorkerService)
                registry.lookup(
                        workerInfo.getServiceName()
                );
    }


    // ==========================================
    // CONNECT TO BOOTSTRAP NEIGHBOUR
    // ==========================================

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

            neighbours.put(
                    neighbourInfo.getWorkerId(),
                    neighbourInfo
            );


            WorkerService neighbour =
                    getRemoteWorker(
                            neighbourInfo
                    );


            WorkerInfo myInfo =
                    new WorkerInfo(
                            workerId,
                            host,
                            registryPort,
                            serviceName
                    );


            /*
             * Make connection two-way.
             */

            neighbour.addNeighbour(
                    myInfo
            );


            System.out.println(
                    "Worker " + workerId +
                            " successfully connected to Worker "
                            + neighbourInfo.getWorkerId()
            );


        } catch (Exception e) {

            System.err.println(
                    "Worker " + workerId +
                            " could not connect to neighbour Worker "
                            + neighbourInfo.getWorkerId()
            );

            e.printStackTrace();
        }
    }


    // ==========================================
    // PRINT WORKER INFORMATION
    // ==========================================

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
                "Coordinator: "
                        + coordinatorId.get()
        );

        System.out.println(
                "======================================"
        );
    }


    // ==========================================
    // MAIN
    // ==========================================

    public static void main(String[] args) {

        /*
         * Worker needs:
         *
         * Worker ID
         * RMI Port
         *
         * Example:
         *
         * 1 2001
         */

        if (args.length < 2) {

            System.out.println(
                    "Usage: WorkerNode <workerId> <port>"
            );

            System.out.println(
                    "Example: WorkerNode 1 2001"
            );

            return;
        }


        try {

            int workerId =
                    Integer.parseInt(
                            args[0]
                    );


            int workerPort =
                    Integer.parseInt(
                            args[1]
                    );


            String workerHost =
                    "localhost";


            String serviceName =
                    "Worker" + workerId;


            /*
             * Create Worker
             */

            WorkerNode worker =
                    new WorkerNode(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            /*
             * Create RMI Registry
             * for this Worker.
             */

            Registry workerRegistry =
                    LocateRegistry.createRegistry(
                            workerPort
                    );


            /*
             * Publish Worker service.
             */

            workerRegistry.rebind(
                    serviceName,
                    worker
            );


            worker.printWorkerDetails();


            // ==================================
            // CONNECT TO BOOTSTRAP
            // ==================================

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


            WorkerInfo myInfo =
                    new WorkerInfo(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            /*
             * Register Worker.
             *
             * Bootstrap returns a random
             * existing active Worker.
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
             * Connect to neighbour.
             */

            worker.connectToNeighbour(
                    neighbour
            );


            System.out.println(
                    "Worker " + workerId +
                            " is now ready."
            );


            // ==================================
            // CLEAN SHUTDOWN
            // ==================================

            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                try {

                                    bootstrap.unregisterWorker(
                                            workerId
                                    );


                                    System.out.println(
                                            "Worker "
                                                    + workerId
                                                    + " removed from Bootstrap."
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