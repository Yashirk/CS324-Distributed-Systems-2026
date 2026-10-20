
package cs324.worker;

import cs324.bootstrap.BootstrapService;
import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.JobType;
import cs324.common.WorkerInfo;

import java.math.BigInteger;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class JobProcessingTest {

    public static void main(String[] args) {

        try {

            Registry registry =
                    LocateRegistry.getRegistry("localhost", 1099);

            BootstrapService bootstrap =
                    (BootstrapService) registry.lookup("BootstrapService");

            List<WorkerInfo> workers = bootstrap.getActiveWorkers();

            if (!bootstrap.isElectionReady()) {
                System.out.println(
                        "Please start all selected workers first."
                );
                return;
            }

            if (workers.isEmpty()) {
                System.out.println("No workers available.");
                return;
            }

            WorkerInfo firstWorker = workers.get(0);

            Registry firstRegistry = LocateRegistry.getRegistry(
                    firstWorker.getHost(),
                    firstWorker.getRegistryPort()
            );

            WorkerService firstService = (WorkerService)
                    firstRegistry.lookup(firstWorker.getServiceName());

            int coordinatorId = firstService.getCoordinatorId();

            if (coordinatorId == -1) {
                System.out.println("No coordinator elected yet.");
                return;
            }

            WorkerService coordinator = null;

            for (WorkerInfo info : workers) {

                if (info.getWorkerId() == coordinatorId) {

                    Registry workerRegistry = LocateRegistry.getRegistry(
                            info.getHost(),
                            info.getRegistryPort()
                    );

                    coordinator = (WorkerService)
                            workerRegistry.lookup(info.getServiceName());

                    break;
                }
            }

            if (coordinator == null) {
                System.out.println("Coordinator not found.");
                return;
            }

            System.out.println("====================================");
            System.out.println("       CS324 JOB PROCESSING TEST");
            System.out.println("====================================");

            System.out.println(
                    "Coordinator: Worker " + coordinatorId
            );

            System.out.println(
                    "Active Workers: " + workers.size()
            );

            System.out.println();

            // =====================================
            // TEST 1: MAX
            // =====================================

            JobRequest maxJob = new JobRequest(
                    JobType.MAX,
                    Arrays.asList(12, 45, 7, 99, 23)
            );

            JobResult maxResult = coordinator.submitJob(maxJob);

            boolean maxPassed =
                    maxResult.getResult().equals(
                            BigInteger.valueOf(99)
                    );

            System.out.println(
                    "MAX Result: " + maxResult.getResult()
            );

            System.out.println(
                    "MAX Test: " + (maxPassed ? "PASSED" : "FAILED")
            );

            System.out.println();

            // =====================================
            // TEST 2: PRIMESUM
            // =====================================

            JobRequest primeSumJob = new JobRequest(
                    JobType.PRIMESUM,
                    2,
                    10
            );

            JobResult primeSumResult =
                    coordinator.submitJob(primeSumJob);

            boolean primeSumPassed =
                    primeSumResult.getResult().equals(
                            BigInteger.valueOf(17)
                    );

            System.out.println(
                    "PRIMESUM Result: " + primeSumResult.getResult()
            );

            System.out.println(
                    "PRIMESUM Test: "
                            + (primeSumPassed ? "PASSED" : "FAILED")
            );

            System.out.println();

            // =====================================
            // TEST 3: PRIMECOUNT
            // =====================================

            JobRequest primeCountJob = new JobRequest(
                    JobType.PRIMECOUNT,
                    Arrays.asList(2, 3, 5, 7, 8, 9)
            );

            JobResult primeCountResult =
                    coordinator.submitJob(primeCountJob);

            boolean primeCountPassed =
                    primeCountResult.getResult().equals(
                            BigInteger.valueOf(4)
                    );

            System.out.println(
                    "PRIMECOUNT Result: " + primeCountResult.getResult()
            );

            System.out.println(
                    "PRIMECOUNT Test: "
                            + (primeCountPassed ? "PASSED" : "FAILED")
            );

            System.out.println();

            // =====================================
            // FINAL RESULTS
            // =====================================

            System.out.println("====================================");

            if (maxPassed && primeSumPassed && primeCountPassed) {
                System.out.println(
                        "ALL JOB PROCESSING TESTS PASSED."
                );
            } else {
                System.out.println(
                        "SOME JOB PROCESSING TESTS FAILED."
                );
            }

            System.out.println("====================================");

        } catch (Exception e) {

            System.err.println(
                    "Job Processing Test Failed: " + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}
