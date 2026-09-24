package cs324.worker;

import cs324.bootstrap.BootstrapService;
import cs324.common.ElectionCandidate;
import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.JobType;
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
import java.util.concurrent.*;
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

    // Required by assignment
    private final String leaderman = "cs324";

    // Job Allocation Counter
    private final AtomicInteger jac =
            new AtomicInteger(0);

    // Current Coordinator
    // -1 = no coordinator elected yet
    private final AtomicInteger coordinatorId =
            new AtomicInteger(-1);

    // Direct neighbours
    private final Map<Integer, WorkerInfo> neighbours =
            new ConcurrentHashMap<>();

    // Prevent duplicate election processing
    private final Set<String> processedElections =
            ConcurrentHashMap.newKeySet();

    // Prevent duplicate coordinator messages
    private final Set<String> processedCoordinatorMessages =
            ConcurrentHashMap.newKeySet();


    // =====================================================
    // THREADING
    // =====================================================

    /*
     * Used when this Worker executes partial jobs.
     * Multiple partial jobs can therefore run concurrently.
     */
    private final ExecutorService workerJobPool =
            Executors.newCachedThreadPool();

    /*
     * Used by the Coordinator to send pieces of a job
     * to multiple workers at the same time.
     */
    private final ExecutorService coordinatorJobPool =
            Executors.newCachedThreadPool();


    // =====================================================
    // COORDINATOR TERM
    // =====================================================

    /*
     * Number of COMPLETE client jobs assigned
     * during the current coordinator term.
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

        int newValue = jac.incrementAndGet();

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

        List<WorkerInfo> list =
                new ArrayList<>(
                        neighbours.values()
                );

        list.sort(
                Comparator.comparingInt(
                        WorkerInfo::getWorkerId
                )
        );

        return list;
    }


    // =====================================================
    // LEADER ELECTION
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


        System.out.println(
                "Candidates discovered:"
        );

        for (ElectionCandidate candidate
                : candidates) {

            System.out.println(
                    candidate
            );
        }


        ElectionCandidate winner = null;


        for (ElectionCandidate candidate
                : candidates) {

            if (winner == null) {

                winner = candidate;

                continue;
            }


            // LOWEST JAC wins
            if (candidate.getJac()
                    < winner.getJac()) {

                winner = candidate;
            }

            // Same JAC -> HIGHEST ID wins
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


        candidates.add(
                new ElectionCandidate(
                        workerId,
                        jac.get()
                )
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


                candidates.addAll(
                        neighbour.processElection(
                                electionId,
                                workerId
                        )
                );


            } catch (Exception e) {

                System.err.println(
                        "Could not send election to Worker "
                                + neighbourInfo.getWorkerId()
                );
            }
        }


        return candidates;
    }


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

        /*
         * Every election begins a new coordinator term.
         */
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
                        "Could not send Coordinator message to Worker "
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
    // FULL JOB SUBMISSION
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
         * may accept complete client jobs.
         */
        if (coordinatorId.get() != workerId) {

            throw new RemoteException(
                    "Worker " + workerId +
                            " is not the Coordinator. Current Coordinator = Worker "
                            + coordinatorId.get()
            );
        }


        int currentTermJob =
                termJobCount.incrementAndGet();


        if (currentTermJob
                > MAX_JOBS_PER_TERM) {

            termJobCount.decrementAndGet();

            throw new RemoteException(
                    "Coordinator term has ended. A new election is required."
            );
        }


        System.out.println();
        System.out.println(
                "======================================"
        );

        System.out.println(
                "Coordinator Worker " + workerId +
                        " received job."
        );

        System.out.println(
                "Job ID: " + job.getJobId()
        );

        System.out.println(
                "Job Type: " + job.getJobType()
        );

        System.out.println(
                "Term Job: "
                        + currentTermJob
                        + "/"
                        + MAX_JOBS_PER_TERM
        );

        System.out.println(
                "======================================"
        );


        long finalResult =
                distributeJob(
                        job
                );


        JobResult result =
                new JobResult(
                        job.getJobId(),
                        finalResult,
                        workerId
                );


        System.out.println(
                "Job completed."
        );

        System.out.println(
                "Final Result = "
                        + finalResult
        );


        /*
         * After 5 complete jobs,
         * the coordinator term ends.
         */
        if (currentTermJob
                == MAX_JOBS_PER_TERM) {

            System.out.println();
            System.out.println(
                    "Coordinator Worker "
                            + workerId
                            + " has completed 5 jobs."
            );

            System.out.println(
                    "Coordinator term ended."
            );

            System.out.println(
                    "Starting new leader election..."
            );


            /*
             * Start the election after
             * finishing Job #5.
             */
            startElection();
        }


        return result;
    }


    // =====================================================
    // DISTRIBUTE JOB
    // =====================================================

    private long distributeJob(
            JobRequest job)
            throws RemoteException {

        try {

            /*
             * Ask Bootstrap for all
             * currently active workers.
             */
            BootstrapService bootstrap =
                    getBootstrapService();


            List<WorkerInfo> workers =
                    bootstrap.getActiveWorkers();


            if (workers.isEmpty()) {

                throw new RemoteException(
                        "No active workers available."
                );
            }


            workers.sort(
                    Comparator.comparingInt(
                            WorkerInfo::getWorkerId
                    )
            );


            if (job.getJobType()
                    == JobType.MAX
                    ||
                    job.getJobType()
                            == JobType.PRIMECOUNT) {

                return distributeNumberListJob(
                        job,
                        workers
                );
            }


            if (job.getJobType()
                    == JobType.PRIMESUM) {

                return distributePrimeSumJob(
                        job,
                        workers
                );
            }


            throw new RemoteException(
                    "Unsupported job type."
            );


        } catch (RemoteException e) {

            throw e;

        } catch (Exception e) {

            throw new RemoteException(
                    "Distributed job processing failed.",
                    e
            );
        }
    }


    // =====================================================
    // SPLIT MAX / PRIMECOUNT
    // =====================================================

    private long distributeNumberListJob(
            JobRequest job,
            List<WorkerInfo> workers)
            throws Exception {

        List<Integer> numbers =
                job.getNumbers();


        if (numbers.isEmpty()) {

            throw new RemoteException(
                    "Number list cannot be empty."
            );
        }


        /*
         * Do not create more pieces
         * than there are numbers.
         */
        int workerCount =
                Math.min(
                        workers.size(),
                        numbers.size()
                );


        List<WorkerInfo> selectedWorkers =
                new ArrayList<>(
                        workers.subList(
                                0,
                                workerCount
                        )
                );


        List<List<Integer>> chunks =
                splitNumbers(
                        numbers,
                        workerCount
                );


        List<Future<Long>> futures =
                new ArrayList<>();


        for (int i = 0;
             i < selectedWorkers.size();
             i++) {

            WorkerInfo target =
                    selectedWorkers.get(i);


            List<Integer> chunk =
                    chunks.get(i);


            JobRequest partialJob =
                    new JobRequest(
                            job.getJobType(),
                            chunk
                    );


            futures.add(
                    dispatchPartialJob(
                            target,
                            partialJob
                    )
            );
        }


        if (job.getJobType()
                == JobType.MAX) {

            long maximum =
                    Long.MIN_VALUE;


            for (Future<Long> future
                    : futures) {

                maximum =
                        Math.max(
                                maximum,
                                future.get()
                        );
            }


            return maximum;
        }


        /*
         * PRIMECOUNT:
         * Add partial counts.
         */
        long totalCount = 0;


        for (Future<Long> future
                : futures) {

            totalCount +=
                    future.get();
        }


        return totalCount;
    }


    // =====================================================
    // SPLIT PRIMESUM
    // =====================================================

    private long distributePrimeSumJob(
            JobRequest job,
            List<WorkerInfo> workers)
            throws Exception {

        int start =
                job.getStart();

        int end =
                job.getEnd();


        if (start > end) {

            throw new RemoteException(
                    "PRIMESUM start cannot be greater than end."
            );
        }


        int totalValues =
                end - start + 1;


        int workerCount =
                Math.min(
                        workers.size(),
                        totalValues
                );


        List<WorkerInfo> selectedWorkers =
                new ArrayList<>(
                        workers.subList(
                                0,
                                workerCount
                        )
                );


        List<int[]> ranges =
                splitRange(
                        start,
                        end,
                        workerCount
                );


        List<Future<Long>> futures =
                new ArrayList<>();


        for (int i = 0;
             i < selectedWorkers.size();
             i++) {

            WorkerInfo target =
                    selectedWorkers.get(i);


            int[] range =
                    ranges.get(i);


            JobRequest partialJob =
                    new JobRequest(
                            JobType.PRIMESUM,
                            range[0],
                            range[1]
                    );


            futures.add(
                    dispatchPartialJob(
                            target,
                            partialJob
                    )
            );
        }


        long totalSum = 0;


        for (Future<Long> future
                : futures) {

            totalSum +=
                    future.get();
        }


        return totalSum;
    }


    // =====================================================
    // SEND ONE PART TO ONE WORKER
    // =====================================================

    private Future<Long> dispatchPartialJob(
            WorkerInfo target,
            JobRequest partialJob) {

        return coordinatorJobPool.submit(
                () -> {

                    /*
                     * If Coordinator gives work
                     * to ANOTHER worker,
                     * increase Coordinator's JAC.
                     *
                     * This follows the assignment's
                     * JAC definition.
                     */
                    if (target.getWorkerId()
                            != workerId) {

                        int newJac =
                                jac.incrementAndGet();

                        System.out.println(
                                "Coordinator Worker "
                                        + workerId
                                        + " allocated work to Worker "
                                        + target.getWorkerId()
                                        + ". JAC = "
                                        + newJac
                        );
                    }


                    if (target.getWorkerId()
                            == workerId) {

                        return executePartialJob(
                                partialJob
                        );
                    }


                    WorkerService remoteWorker =
                            getRemoteWorker(
                                    target
                            );


                    return remoteWorker
                            .executePartialJob(
                                    partialJob
                            );
                }
        );
    }


    // =====================================================
    // WORKER EXECUTES PARTIAL JOB
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

            /*
             * Explicit Java thread execution.
             */
            Future<Long> future =
                    workerJobPool.submit(
                            () ->
                                    calculatePartialJob(
                                            partialJob
                                    )
                    );


            return future.get();


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
    // ACTUAL CALCULATIONS
    // =====================================================

    private long calculatePartialJob(
            JobRequest job)
            throws RemoteException {

        System.out.println(
                "Worker " + workerId +
                        " processing partial "
                        + job.getJobType()
                        + " job."
        );


        switch (job.getJobType()) {

            case MAX:
                return calculateMax(
                        job.getNumbers()
                );

            case PRIMECOUNT:
                return calculatePrimeCount(
                        job.getNumbers()
                );

            case PRIMESUM:
                return calculatePrimeSum(
                        job.getStart(),
                        job.getEnd()
                );

            default:
                throw new RemoteException(
                        "Unsupported job type."
                );
        }
    }


    private long calculateMax(
            List<Integer> numbers)
            throws RemoteException {

        if (numbers.isEmpty()) {

            throw new RemoteException(
                    "MAX list cannot be empty."
            );
        }


        long maximum =
                numbers.get(0);


        for (int number : numbers) {

            if (number > maximum) {

                maximum =
                        number;
            }
        }


        return maximum;
    }


    private long calculatePrimeCount(
            List<Integer> numbers) {

        long count = 0;


        for (int number : numbers) {

            if (isPrime(number)) {

                count++;
            }
        }


        return count;
    }


    private long calculatePrimeSum(
            int start,
            int end) {

        long sum = 0;


        for (int number = start;
             number <= end;
             number++) {

            if (isPrime(number)) {

                sum += number;
            }
        }


        return sum;
    }


    private boolean isPrime(
            int number) {

        if (number < 2) {

            return false;
        }


        if (number == 2) {

            return true;
        }


        if (number % 2 == 0) {

            return false;
        }


        for (int i = 3;
             (long) i * i <= number;
             i += 2) {

            if (number % i == 0) {

                return false;
            }
        }


        return true;
    }


    // =====================================================
    // SPLITTING HELPERS
    // =====================================================

    private List<List<Integer>> splitNumbers(
            List<Integer> numbers,
            int parts) {

        List<List<Integer>> result =
                new ArrayList<>();


        int total =
                numbers.size();

        int baseSize =
                total / parts;

        int remainder =
                total % parts;

        int index = 0;


        for (int i = 0;
             i < parts;
             i++) {

            int thisPartSize =
                    baseSize
                            + (i < remainder
                            ? 1
                            : 0);


            List<Integer> chunk =
                    new ArrayList<>(
                            numbers.subList(
                                    index,
                                    index
                                            + thisPartSize
                            )
                    );


            result.add(
                    chunk
            );


            index +=
                    thisPartSize;
        }


        return result;
    }


    private List<int[]> splitRange(
            int start,
            int end,
            int parts) {

        List<int[]> ranges =
                new ArrayList<>();


        int total =
                end - start + 1;

        int baseSize =
                total / parts;

        int remainder =
                total % parts;

        int currentStart =
                start;


        for (int i = 0;
             i < parts;
             i++) {

            int thisPartSize =
                    baseSize
                            + (i < remainder
                            ? 1
                            : 0);


            int currentEnd =
                    currentStart
                            + thisPartSize
                            - 1;


            ranges.add(
                    new int[]{
                            currentStart,
                            currentEnd
                    }
            );


            currentStart =
                    currentEnd + 1;
        }


        return ranges;
    }


    // =====================================================
    // RMI HELPERS
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


    private BootstrapService getBootstrapService()
            throws Exception {

        Registry bootstrapRegistry =
                LocateRegistry.getRegistry(
                        "localhost",
                        1099
                );


        return (BootstrapService)
                bootstrapRegistry.lookup(
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


    // =====================================================
    // DISPLAY WORKER INFORMATION
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


            WorkerNode worker =
                    new WorkerNode(
                            workerId,
                            workerHost,
                            workerPort,
                            serviceName
                    );


            Registry workerRegistry =
                    LocateRegistry.createRegistry(
                            workerPort
                    );


            workerRegistry.rebind(
                    serviceName,
                    worker
            );


            worker.printWorkerDetails();


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
                    "Worker " + workerId +
                            " registered with Bootstrap successfully."
            );


            worker.connectToNeighbour(
                    neighbour
            );


            System.out.println(
                    "Worker " + workerId +
                            " is now ready."
            );


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