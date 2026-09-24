package cs324.worker;

import cs324.bootstrap.BootstrapService;
import cs324.common.ElectionCandidate;
import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.WorkerInfo;
import cs324.job.JobCalculator;
import cs324.job.JobDistributor;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.concurrent.atomic.AtomicInteger;


public class WorkerNode extends UnicastRemoteObject
        implements WorkerService {

    // =====================================================
    // BASIC WORKER INFORMATION
    // =====================================================

    private final int workerId;
    private final String host;
    private final int registryPort;
    private final String serviceName;

    // Required variable
    private final String leaderman = "cs324";

    // Job Allocation Counter
    private final AtomicInteger jac =
            new AtomicInteger(0);

    // -1 means no coordinator elected yet
    private final AtomicInteger coordinatorId =
            new AtomicInteger(-1);

    // Direct neighbours
    private final Map<Integer, WorkerInfo> neighbours =
            new ConcurrentHashMap<>();


    // =====================================================
    // ELECTION TRACKING
    // =====================================================

    // Prevent processing same election twice
    private final Set<String> processedElections =
            ConcurrentHashMap.newKeySet();

    // Prevent processing same coordinator message twice
    private final Set<String> processedCoordinatorMessages =
            ConcurrentHashMap.newKeySet();


    // =====================================================
    // THREADING
    // =====================================================

    // Worker calculations
    private final ExecutorService workerJobPool =
            Executors.newCachedThreadPool();

    // Coordinator distributes jobs concurrently
    private final ExecutorService coordinatorJobPool =
            Executors.newCachedThreadPool();


    // =====================================================
    // COORDINATOR TERM
    // =====================================================

    private final AtomicInteger termJobCount =
            new AtomicInteger(0);

    private static final int MAX_JOBS_PER_TERM = 5;


    // =====================================================
    // CONSTRUCTOR
    // =====================================================

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


    // =====================================================
    // BASIC WORKER METHODS
    // =====================================================

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

        int value = jac.incrementAndGet();

        System.out.println(
                "Worker " + workerId +
                        " JAC increased to " + value
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

        List<WorkerInfo> result =
                new ArrayList<>(
                        neighbours.values()
                );

        result.sort(
                Comparator.comparingInt(
                        WorkerInfo::getWorkerId
                )
        );

        return result;
    }


    // =====================================================
    // START LEADER ELECTION
    // =====================================================

    @Override
    public void startElection()
            throws RemoteException {

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


        List<ElectionCandidate> candidates =
                processElection(
                        electionId,
                        -1
                );


        if (candidates.isEmpty()) {

            System.out.println(
                    "No election candidates found."
            );

            return;
        }


        ElectionCandidate winner = null;


        for (ElectionCandidate candidate : candidates) {

            if (winner == null) {

                winner = candidate;

                continue;
            }


            // Lowest JAC wins
            if (candidate.getJac()
                    < winner.getJac()) {

                winner = candidate;
            }

            // If JAC tied, highest Worker ID wins
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


        announceCoordinator(
                electionId,
                winner.getWorkerId(),
                -1
        );
    }


    // =====================================================
    // PROCESS ELECTION
    // =====================================================

    @Override
    public List<ElectionCandidate> processElection(
            String electionId,
            int senderId)
            throws RemoteException {

        boolean firstTime =
                processedElections.add(
                        electionId
                );


        if (!firstTime) {

            System.out.println(
                    "Worker " + workerId +
                            " ignored duplicate election."
            );

            return new ArrayList<>();
        }


        System.out.println(
                "Worker " + workerId +
                        " processing ELECTION message."
        );


        List<ElectionCandidate> candidates =
                new ArrayList<>();


        // Add this worker
        candidates.add(
                new ElectionCandidate(
                        workerId,
                        jac.get()
                )
        );


        // Send election to neighbours
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


                candidates.addAll(
                        neighbour.processElection(
                                electionId,
                                workerId
                        )
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


    // =====================================================
    // COORDINATOR MESSAGE
    // =====================================================

    @Override
    public void announceCoordinator(
            String electionId,
            int newCoordinatorId,
            int senderId)
            throws RemoteException {

        boolean firstTime =
                processedCoordinatorMessages.add(
                        electionId
                );


        if (!firstTime) {
            return;
        }


        coordinatorId.set(
                newCoordinatorId
        );

        // New election = new term
        termJobCount.set(0);


        System.out.println();
        System.out.println(
                "Worker " + workerId +
                        " received COORDINATOR message."
        );

        System.out.println(
                "Current Coordinator = Worker "
                        + newCoordinatorId
        );


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
                                " could not send Coordinator message to Worker "
                                + neighbourInfo.getWorkerId()
                );
            }
        }
    }


    @Override
    public int getCoordinatorId()
            throws RemoteException {

        return coordinatorId.get();
    }


    // =====================================================
    // COMPLETE JOB FROM CLIENT
    // =====================================================

    @Override
    public JobResult submitJob(
            JobRequest job)
            throws RemoteException {

        if (job == null) {

            throw new RemoteException(
                    "Job cannot be null."
            );
        }


        // Only elected Coordinator accepts full jobs
        if (coordinatorId.get() != workerId) {

            throw new RemoteException(
                    "Worker " + workerId +
                            " is not the Coordinator. "
                            + "Current Coordinator = Worker "
                            + coordinatorId.get()
            );
        }


        int jobNumber =
                termJobCount.incrementAndGet();


        if (jobNumber > MAX_JOBS_PER_TERM) {

            termJobCount.decrementAndGet();

            throw new RemoteException(
                    "Coordinator term has ended. "
                            + "A new election is required."
            );
        }


        System.out.println();
        System.out.println(
                "======================================"
        );

        System.out.println(
                "Coordinator Worker "
                        + workerId
                        + " received job."
        );

        System.out.println(
                "Job Type: "
                        + job.getJobType()
        );

        System.out.println(
                "Term Job: "
                        + jobNumber
                        + "/"
                        + MAX_JOBS_PER_TERM
        );

        System.out.println(
                "======================================"
        );


        try {

            BootstrapService bootstrap =
                    getBootstrapService();


            List<WorkerInfo> activeWorkers =
                    bootstrap.getActiveWorkers();


            if (activeWorkers.isEmpty()) {

                throw new RemoteException(
                        "No active workers available."
                );
            }


            activeWorkers.sort(
                    Comparator.comparingInt(
                            WorkerInfo::getWorkerId
                    )
            );


            // Split full job evenly
            List<JobRequest> partialJobs =
                    JobDistributor.splitJob(
                            job,
                            activeWorkers.size()
                    );


            List<Future<Long>> futures =
                    new ArrayList<>();


            for (int i = 0;
                 i < partialJobs.size();
                 i++) {


                WorkerInfo targetWorker =
                        activeWorkers.get(i);


                JobRequest partialJob =
                        partialJobs.get(i);


                Future<Long> future =
                        coordinatorJobPool.submit(
                                () -> {

                                    /*
                                     * JAC increases when
                                     * Coordinator assigns work
                                     * to ANOTHER worker.
                                     */
                                    if (targetWorker.getWorkerId()
                                            != workerId) {

                                        int newJac =
                                                jac.incrementAndGet();


                                        System.out.println(
                                                "Coordinator Worker "
                                                        + workerId
                                                        + " allocated work to Worker "
                                                        + targetWorker.getWorkerId()
                                                        + ". JAC = "
                                                        + newJac
                                        );
                                    }


                                    // Coordinator may also process a part
                                    if (targetWorker.getWorkerId()
                                            == workerId) {

                                        return executePartialJob(
                                                partialJob
                                        );
                                    }


                                    WorkerService remoteWorker =
                                            getRemoteWorker(
                                                    targetWorker
                                            );


                                    return remoteWorker
                                            .executePartialJob(
                                                    partialJob
                                            );
                                }
                        );


                futures.add(
                        future
                );
            }


            // Collect partial answers
            List<Long> partialResults =
                    new ArrayList<>();


            for (Future<Long> future : futures) {

                partialResults.add(
                        future.get()
                );
            }


            // Combine answers
            long finalAnswer =
                    JobDistributor.combineResults(
                            job.getJobType(),
                            partialResults
                    );


            System.out.println();
            System.out.println(
                    "Job completed."
            );

            System.out.println(
                    "Final Result = "
                            + finalAnswer
            );


            JobResult result =
                    new JobResult(
                            job.getJobId(),
                            finalAnswer,
                            workerId
                    );


            /*
             * Coordinator can allocate
             * maximum 5 complete jobs.
             */
            if (jobNumber
                    == MAX_JOBS_PER_TERM) {


                System.out.println();
                System.out.println(
                        "Coordinator Worker "
                                + workerId
                                + " completed 5 jobs."
                );

                System.out.println(
                        "Coordinator term ended."
                );

                System.out.println(
                        "Starting a new leader election..."
                );


                // Start next election asynchronously
                new Thread(() -> {

                    try {

                        startElection();

                    } catch (Exception e) {

                        System.err.println(
                                "Could not start new election."
                        );
                    }

                }).start();
            }


            return result;


        } catch (RemoteException e) {

            throw e;


        } catch (Exception e) {

            throw new RemoteException(
                    "Job processing failed.",
                    e
            );
        }
    }


    // =====================================================
    // PARTIAL JOB EXECUTION
    // =====================================================

    @Override
    public long executePartialJob(
            JobRequest partialJob)
            throws RemoteException {

        if (partialJob == null) {

            throw new RemoteException(
                    "Partial job cannot be null."
            );
        }


        try {

            // Run calculation in Java thread pool
            Future<Long> future =
                    workerJobPool.submit(
                            () ->
                                    JobCalculator.calculate(
                                            partialJob
                                    )
                    );


            long answer =
                    future.get();


            System.out.println(
                    "Worker " + workerId
                            + " completed partial "
                            + partialJob.getJobType()
                            + " job. Result = "
                            + answer
            );


            return answer;


        } catch (InterruptedException e) {

            Thread.currentThread()
                    .interrupt();


            throw new RemoteException(
                    "Partial job interrupted.",
                    e
            );


        } catch (ExecutionException e) {

            throw new RemoteException(
                    "Partial job failed.",
                    e.getCause()
            );
        }
    }


    // =====================================================
    // CONNECT TO REMOTE WORKER
    // =====================================================

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


    // =====================================================
    // CONNECT TO BOOTSTRAP
    // =====================================================

    private BootstrapService getBootstrapService()
            throws Exception {


        Registry registry =
                LocateRegistry.getRegistry(
                        "localhost",
                        1099
                );


        return (BootstrapService)
                registry.lookup(
                        "BootstrapService"
                );
    }


    // =====================================================
    // CONNECT TO RANDOM NEIGHBOUR
    // =====================================================

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


            // Make neighbour relationship two-way
            neighbour.addNeighbour(
                    myInfo
            );


            System.out.println(
                    "Worker " + workerId
                            + " successfully connected to Worker "
                            + neighbourInfo.getWorkerId()
            );


        } catch (Exception e) {

            System.err.println(
                    "Worker " + workerId
                            + " could not connect to Worker "
                            + neighbourInfo.getWorkerId()
            );

            e.printStackTrace();
        }
    }


    // =====================================================
    // DISPLAY WORKER DETAILS
    // =====================================================

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


    // =====================================================
    // MAIN
    // =====================================================

    public static void main(
            String[] args) {


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


            // Create Worker
            WorkerNode worker =
                    new WorkerNode(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            // Start Worker's RMI Registry
            Registry workerRegistry =
                    LocateRegistry.createRegistry(
                            workerPort
                    );


            workerRegistry.rebind(
                    serviceName,
                    worker
            );


            worker.printWorkerDetails();


            // Connect to Bootstrap
            BootstrapService bootstrap =
                    worker.getBootstrapService();


            WorkerInfo myInfo =
                    new WorkerInfo(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            WorkerInfo neighbour =
                    bootstrap.registerWorker(
                            myInfo
                    );


            System.out.println(
                    "Worker " + workerId
                            + " registered with Bootstrap successfully."
            );


            // Join Worker network
            worker.connectToNeighbour(
                    neighbour
            );


            System.out.println(
                    "Worker " + workerId
                            + " is now ready."
            );


            // Clean shutdown
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(() -> {

                                try {

                                    bootstrap.unregisterWorker(
                                            workerId
                                    );


                                    worker.workerJobPool
                                            .shutdownNow();


                                    worker.coordinatorJobPool
                                            .shutdownNow();


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