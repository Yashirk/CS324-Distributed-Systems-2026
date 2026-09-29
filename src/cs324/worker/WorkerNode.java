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


public class WorkerNode
        extends UnicastRemoteObject
        implements WorkerService {


    // =====================================================
    // WORKER INFORMATION
    // =====================================================

    private final int workerId;

    private final String host;

    private final int registryPort;

    private final String serviceName;


    // =====================================================
    // JOB ALLOCATION COUNTER
    // =====================================================

    /*
     * JAC = Job Allocation Counter
     *
     * It increases when this Worker is Coordinator
     * and assigns work to another Worker.
     */
    private final AtomicInteger jac =
            new AtomicInteger(0);


    // =====================================================
    // CURRENT COORDINATOR
    // =====================================================

    /*
     * -1 means no Coordinator has been elected yet.
     */
    private final AtomicInteger coordinatorId =
            new AtomicInteger(-1);


    // =====================================================
    // NEIGHBOURS
    // =====================================================

    /*
     * Key   = Worker ID
     * Value = Worker information
     */
    private final Map<Integer, WorkerInfo> neighbours =
            new ConcurrentHashMap<>();


    // =====================================================
    // DUPLICATE MESSAGE PROTECTION
    // =====================================================

    /*
     * Prevents the same election message
     * being processed more than once.
     */
    private final Set<String> processedElections =
            ConcurrentHashMap.newKeySet();


    /*
     * Prevents the same Coordinator announcement
     * being processed repeatedly.
     */
    private final Set<String> processedCoordinatorMessages =
            ConcurrentHashMap.newKeySet();


    // =====================================================
    // THREAD POOLS
    // =====================================================

    /*
     * Used by this Worker when processing
     * partial computational jobs.
     */
    private final ExecutorService workerJobPool =
            Executors.newCachedThreadPool();


    /*
     * Used when this Worker is Coordinator
     * and distributes jobs to Workers.
     */
    private final ExecutorService coordinatorJobPool =
            Executors.newCachedThreadPool();


    // =====================================================
    // COORDINATOR TERM
    // =====================================================

    /*
     * Counts the number of complete jobs handled
     * during the current Coordinator term.
     */
    private final AtomicInteger termJobCount =
            new AtomicInteger(0);


    private static final int MAX_JOBS_PER_TERM = 5;


    // =====================================================
    // CONSTRUCTOR
    // =====================================================

    public WorkerNode(
            int workerId,
            String host,
            int registryPort)
            throws RemoteException {

        super();

        this.workerId =
                workerId;

        this.host =
                host;

        this.registryPort =
                registryPort;

        this.serviceName =
                "Worker" + workerId;
    }


    // =====================================================
    // GET WORKER ID
    // =====================================================

    @Override
    public int getWorkerId()
            throws RemoteException {

        return workerId;
    }


    // =====================================================
    // GET JAC
    // =====================================================

    @Override
    public int getJAC()
            throws RemoteException {

        return jac.get();
    }


    // =====================================================
    // INCREMENT JAC
    // =====================================================

    @Override
    public void incrementJAC()
            throws RemoteException {

        int newValue =
                jac.incrementAndGet();


        System.out.println(
                "Worker "
                        + workerId
                        + " JAC increased to "
                        + newValue
        );
    }


    // =====================================================
    // ADD NEIGHBOUR
    // =====================================================

    @Override
    public void addNeighbour(
            WorkerInfo worker)
            throws RemoteException {


        if (worker == null) {
            return;
        }


        /*
         * A Worker must not add itself
         * as its own neighbour.
         */
        if (worker.getWorkerId()
                == workerId) {

            return;
        }


        neighbours.put(
                worker.getWorkerId(),
                worker
        );


        System.out.println(
                "Worker "
                        + workerId
                        + " added Worker "
                        + worker.getWorkerId()
                        + " as neighbour."
        );
    }


    // =====================================================
    // GET NEIGHBOURS
    // =====================================================

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
                UUID.randomUUID()
                        .toString();


        System.out.println();
        System.out.println(
                "========================================"
        );

        System.out.println(
                "Worker "
                        + workerId
                        + " STARTING LEADER ELECTION"
        );

        System.out.println(
                "Election ID: "
                        + electionId
        );

        System.out.println(
                "========================================"
        );


        /*
         * Propagate the election and collect
         * all reachable candidates.
         */
        List<ElectionCandidate> candidates =
                processElection(
                        electionId,
                        -1
                );


        if (candidates.isEmpty()) {

            throw new RemoteException(
                    "No election candidates were found."
            );
        }


        /*
         * Election rule:
         *
         * 1. Lowest JAC wins.
         * 2. If equal JAC, highest Worker ID wins.
         */
        ElectionCandidate winner =
                candidates.stream()
                        .min(
                                Comparator
                                        .comparingInt(
                                                ElectionCandidate::getJac
                                        )
                                        .thenComparing(
                                                Comparator.comparingInt(
                                                        ElectionCandidate::getWorkerId
                                                ).reversed()
                                        )
                        )
                        .orElseThrow(
                                () ->
                                        new RemoteException(
                                                "Unable to select Coordinator."
                                        )
                        );


        System.out.println();
        System.out.println(
                "Election candidates:"
        );


        for (ElectionCandidate candidate
                : candidates) {

            System.out.println(
                    "Worker "
                            + candidate.getWorkerId()
                            + " | JAC = "
                            + candidate.getJac()
            );
        }


        System.out.println();
        System.out.println(
                "ELECTED COORDINATOR: Worker "
                        + winner.getWorkerId()
        );


        /*
         * Propagate the election result
         * throughout the network.
         */
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


        List<ElectionCandidate> candidates =
                new ArrayList<>();


        /*
         * If this Worker has already processed
         * this election, stop here.
         */
        if (!processedElections.add(
                electionId)) {

            return candidates;
        }


        System.out.println(
                "Worker "
                        + workerId
                        + " processing election "
                        + electionId
        );


        /*
         * Add this Worker's election information.
         */
        candidates.add(
                new ElectionCandidate(
                        workerId,
                        jac.get()
                )
        );


        /*
         * Forward election to neighbours.
         */
        for (WorkerInfo neighbour
                : neighbours.values()) {


            /*
             * Do not immediately send the message
             * back to the Worker that sent it.
             */
            if (neighbour.getWorkerId()
                    == senderId) {

                continue;
            }


            try {

                WorkerService remoteWorker =
                        getRemoteWorker(
                                neighbour
                        );


                List<ElectionCandidate>
                        remoteCandidates =
                        remoteWorker.processElection(
                                electionId,
                                workerId
                        );


                if (remoteCandidates != null) {

                    candidates.addAll(
                            remoteCandidates
                    );
                }


            } catch (Exception e) {

                System.err.println(
                        "Worker "
                                + workerId
                                + " could not send election to Worker "
                                + neighbour.getWorkerId()
                );
            }
        }


        return candidates;
    }


    // =====================================================
    // ANNOUNCE COORDINATOR
    // =====================================================

    @Override
    public void announceCoordinator(
            String electionId,
            int newCoordinatorId,
            int senderId)
            throws RemoteException {


        /*
         * Prevent duplicate Coordinator messages.
         */
        if (!processedCoordinatorMessages.add(
                electionId)) {

            return;
        }


        coordinatorId.set(
                newCoordinatorId
        );


        /*
         * A new Coordinator means a new term.
         */
        termJobCount.set(
                0
        );


        System.out.println(
                "Worker "
                        + workerId
                        + " recognises Worker "
                        + newCoordinatorId
                        + " as Coordinator."
        );


        /*
         * Forward Coordinator message
         * through the Worker graph.
         */
        for (WorkerInfo neighbour
                : neighbours.values()) {


            if (neighbour.getWorkerId()
                    == senderId) {

                continue;
            }


            try {

                WorkerService remoteWorker =
                        getRemoteWorker(
                                neighbour
                        );


                remoteWorker.announceCoordinator(
                        electionId,
                        newCoordinatorId,
                        workerId
                );


            } catch (Exception e) {

                System.err.println(
                        "Worker "
                                + workerId
                                + " could not send Coordinator message to Worker "
                                + neighbour.getWorkerId()
                );
            }
        }
    }


    // =====================================================
    // GET COORDINATOR
    // =====================================================

    @Override
    public int getCoordinatorId()
            throws RemoteException {

        return coordinatorId.get();
    }


    // =====================================================
    // SUBMIT COMPLETE JOB
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


        /*
         * Only the elected Coordinator
         * may accept Client jobs.
         */
        if (coordinatorId.get()
                != workerId) {

            throw new RemoteException(
                    "Worker "
                            + workerId
                            + " is not the Coordinator. "
                            + "Current Coordinator is Worker "
                            + coordinatorId.get()
            );
        }


        /*
         * Count this complete job.
         */
        int jobNumber =
                termJobCount.incrementAndGet();


        /*
         * Maximum of five jobs per term.
         */
        if (jobNumber
                > MAX_JOBS_PER_TERM) {

            termJobCount.decrementAndGet();

            throw new RemoteException(
                    "Coordinator term has ended. "
                            + "A new election is required."
            );
        }


        System.out.println();
        System.out.println(
                "========================================"
        );

        System.out.println(
                "Coordinator Worker "
                        + workerId
                        + " received "
                        + job.getJobType()
                        + " job."
        );

        System.out.println(
                "Term Job: "
                        + jobNumber
                        + "/"
                        + MAX_JOBS_PER_TERM
        );

        System.out.println(
                "Job ID: "
                        + job.getJobId()
        );

        System.out.println(
                "========================================"
        );


        /*
         * Ask Bootstrap for all active Workers.
         */
        BootstrapService bootstrap =
                getBootstrapService();


        List<WorkerInfo> activeWorkers =
                bootstrap.getActiveWorkers();


        if (activeWorkers == null
                ||
                activeWorkers.isEmpty()) {

            throw new RemoteException(
                    "No active Workers are available."
            );
        }


        activeWorkers.sort(
                Comparator.comparingInt(
                        WorkerInfo::getWorkerId
                )
        );


        /*
         * Split the job as evenly as possible.
         */
        List<JobRequest> partialJobs =
                JobDistributor.splitJob(
                        job,
                        activeWorkers.size()
                );


        if (partialJobs == null
                ||
                partialJobs.isEmpty()) {

            throw new RemoteException(
                    "Job could not be divided."
            );
        }


        List<Future<Long>> futures =
                new ArrayList<>();


        /*
         * Assign each partial job to a Worker.
         */
        for (int i = 0;
             i < partialJobs.size();
             i++) {


            final JobRequest partialJob =
                    partialJobs.get(i);


            final WorkerInfo targetWorker =
                    activeWorkers.get(i);


            /*
             * JAC increases when the Coordinator
             * assigns work to another Worker.
             */
            if (targetWorker.getWorkerId()
                    != workerId) {

                incrementJAC();
            }


            Future<Long> future =
                    coordinatorJobPool.submit(
                            () -> {


                                System.out.println(
                                        "Coordinator Worker "
                                                + workerId
                                                + " assigning partial job to Worker "
                                                + targetWorker.getWorkerId()
                                );


                                /*
                                 * If the Coordinator itself receives
                                 * the partial job, execute locally.
                                 */
                                if (targetWorker.getWorkerId()
                                        == workerId) {

                                    return executePartialJob(
                                            partialJob
                                    );
                                }


                                /*
                                 * Otherwise send through RMI.
                                 */
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


        /*
         * Collect all partial results.
         */
        List<Long> partialResults =
                new ArrayList<>();


        for (Future<Long> future
                : futures) {


            try {

                partialResults.add(
                        future.get()
                );


            } catch (InterruptedException e) {

                Thread.currentThread()
                        .interrupt();


                throw new RemoteException(
                        "Job processing was interrupted.",
                        e
                );


            } catch (ExecutionException e) {

                throw new RemoteException(
                        "A Worker failed while processing the job.",
                        e.getCause()
                );
            }
        }


        /*
         * Combine the partial answers.
         */
        long finalResult =
                JobDistributor.combineResults(
                        job.getJobType(),
                        partialResults
                );


        JobResult result =
                new JobResult(
                        job.getJobId(),
                        finalResult,
                        workerId
                );


        System.out.println();
        System.out.println(
                "FINAL RESULT = "
                        + finalResult
        );


        System.out.println(
                "Completed by Coordinator Worker "
                        + workerId
        );


        /*
         * After Job 5, end this Coordinator term
         * and automatically start another election.
         */
        if (jobNumber
                == MAX_JOBS_PER_TERM) {


            System.out.println();
            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "Coordinator Worker "
                            + workerId
                            + " has completed 5 jobs."
            );

            System.out.println(
                    "Coordinator term has ended."
            );

            System.out.println(
                    "Starting new leader election..."
            );

            System.out.println(
                    "========================================"
            );


            /*
             * Start election asynchronously so the
             * fifth job result can still return
             * to the Client immediately.
             */
            new Thread(
                    () -> {

                        try {

                            startElection();

                        } catch (RemoteException e) {

                            System.err.println(
                                    "Automatic leader election failed."
                            );

                            e.printStackTrace();
                        }
                    }
            ).start();
        }


        return result;
    }


    // =====================================================
    // EXECUTE PARTIAL JOB
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


        System.out.println(
                "Worker "
                        + workerId
                        + " processing partial "
                        + partialJob.getJobType()
                        + " job."
        );


        Future<Long> future =
                workerJobPool.submit(
                        () ->
                                JobCalculator.calculate(
                                        partialJob
                                )
                );


        try {

            long result =
                    future.get();


            System.out.println(
                    "Worker "
                            + workerId
                            + " partial result = "
                            + result
            );


            return result;


        } catch (InterruptedException e) {

            Thread.currentThread()
                    .interrupt();


            throw new RemoteException(
                    "Worker "
                            + workerId
                            + " job was interrupted.",
                    e
            );


        } catch (ExecutionException e) {

            throw new RemoteException(
                    "Worker "
                            + workerId
                            + " failed to process partial job.",
                    e.getCause()
            );
        }
    }


    // =====================================================
    // CONNECT TO REMOTE WORKER
    // =====================================================

    private WorkerService getRemoteWorker(
            WorkerInfo worker)
            throws Exception {


        Registry registry =
                LocateRegistry.getRegistry(
                        worker.getHost(),
                        worker.getRegistryPort()
                );


        return (WorkerService)
                registry.lookup(
                        worker.getServiceName()
                );
    }


    // =====================================================
    // CONNECT TO BOOTSTRAP
    // =====================================================

    private BootstrapService getBootstrapService()
            throws RemoteException {


        try {

            Registry registry =
                    LocateRegistry.getRegistry(
                            "localhost",
                            1099
                    );


            return (BootstrapService)
                    registry.lookup(
                            "BootstrapService"
                    );


        } catch (Exception e) {

            throw new RemoteException(
                    "Unable to connect to Bootstrap Node.",
                    e
            );
        }
    }


    // =====================================================
    // THIS WORKER'S INFO
    // =====================================================

    private WorkerInfo getWorkerInfo() {

        return new WorkerInfo(
                workerId,
                host,
                registryPort,
                serviceName
        );
    }


    // =====================================================
    // CONNECT TO NEIGHBOUR
    // =====================================================

    private void connectToNeighbour(
            WorkerInfo neighbour) {


        if (neighbour == null) {

            System.out.println(
                    "Worker "
                            + workerId
                            + " has no initial neighbour."
            );

            return;
        }


        if (neighbour.getWorkerId()
                == workerId) {

            return;
        }


        try {

            /*
             * Add neighbour locally.
             */
            addNeighbour(
                    neighbour
            );


            /*
             * Connect remotely.
             */
            WorkerService remoteWorker =
                    getRemoteWorker(
                            neighbour
                    );


            /*
             * Add this Worker as neighbour
             * on the other Worker as well.
             */
            remoteWorker.addNeighbour(
                    getWorkerInfo()
            );


            System.out.println(
                    "Worker "
                            + workerId
                            + " successfully connected to Worker "
                            + neighbour.getWorkerId()
            );


        } catch (Exception e) {

            System.err.println(
                    "Worker "
                            + workerId
                            + " failed to connect to Worker "
                            + neighbour.getWorkerId()
            );

            e.printStackTrace();
        }
    }


    // =====================================================
    // PRINT WORKER DETAILS
    // =====================================================

    private void printWorkerDetails() {


        System.out.println();
        System.out.println(
                "========================================"
        );

        System.out.println(
                "             CS324 WORKER NODE"
        );

        System.out.println(
                "========================================"
        );

        System.out.println(
                "Worker ID: "
                        + workerId
        );

        System.out.println(
                "Host: "
                        + host
        );

        System.out.println(
                "Registry Port: "
                        + registryPort
        );

        System.out.println(
                "Service Name: "
                        + serviceName
        );

        System.out.println(
                "JAC: "
                        + jac.get()
        );

        System.out.println(
                "Coordinator: "
                        + coordinatorId.get()
        );

        System.out.println(
                "Neighbours: "
                        + neighbours.keySet()
        );

        System.out.println(
                "========================================"
        );
    }


    // =====================================================
    // SHUTDOWN
    // =====================================================

    private void shutdown() {


        System.out.println(
                "Shutting down Worker "
                        + workerId
                        + "..."
        );


        try {

            BootstrapService bootstrap =
                    getBootstrapService();


            bootstrap.unregisterWorker(
                    workerId
            );


        } catch (Exception ignored) {

        }


        workerJobPool.shutdownNow();

        coordinatorJobPool.shutdownNow();
    }


    // =====================================================
    // MAIN METHOD
    // =====================================================

    public static void main(
            String[] args) {


        /*
         * Required arguments:
         *
         * Worker 1:
         * 1 2001
         *
         * Worker 2:
         * 2 2002
         *
         * Worker 3:
         * 3 2003
         */
        if (args.length < 2) {

            System.err.println(
                    "Usage: WorkerNode <workerId> <registryPort>"
            );

            System.err.println(
                    "Example: WorkerNode 1 2001"
            );

            return;
        }


        try {

            int workerId =
                    Integer.parseInt(
                            args[0]
                    );


            int registryPort =
                    Integer.parseInt(
                            args[1]
                    );


            String host =
                    "localhost";


            /*
             * Create Worker object.
             */
            WorkerNode worker =
                    new WorkerNode(
                            workerId,
                            host,
                            registryPort
                    );


            /*
             * Start this Worker's own
             * RMI registry.
             */
            Registry workerRegistry =
                    LocateRegistry.createRegistry(
                            registryPort
                    );


            /*
             * Register Worker service.
             */
            workerRegistry.rebind(
                    worker.serviceName,
                    worker
            );


            /*
             * Connect to Bootstrap.
             */
            BootstrapService bootstrap =
                    worker.getBootstrapService();


            /*
             * Register this Worker.
             *
             * Bootstrap returns a random existing
             * Worker if one is available.
             */
            WorkerInfo initialNeighbour =
                    bootstrap.registerWorker(
                            worker.getWorkerInfo()
                    );


            /*
             * Connect to returned neighbour.
             */
            worker.connectToNeighbour(
                    initialNeighbour
            );


            worker.printWorkerDetails();


            System.out.println(
                    "Worker "
                            + workerId
                            + " is running and waiting for requests."
            );


            /*
             * Remove Worker from Bootstrap
             * when the process is stopped.
             */
            Runtime
                    .getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    worker::shutdown
                            )
                    );


        } catch (NumberFormatException e) {

            System.err.println(
                    "Worker ID and Registry Port must be integers."
            );


        } catch (Exception e) {

            System.err.println(
                    "Failed to start Worker Node."
            );

            e.printStackTrace();
        }
    }
}