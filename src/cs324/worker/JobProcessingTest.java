package cs324.worker;

import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.JobType;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class JobProcessingTest {

    public static void main(String[] args) {

        try {

            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "CS324 DISTRIBUTED JOB TEST"
            );

            System.out.println(
                    "======================================"
            );


            // Connect to Worker 1
            Registry registry1 =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2001
                    );

            WorkerService worker1 =
                    (WorkerService)
                            registry1.lookup(
                                    "Worker1"
                            );


            // Connect to Worker 2
            Registry registry2 =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2002
                    );

            WorkerService worker2 =
                    (WorkerService)
                            registry2.lookup(
                                    "Worker2"
                            );


            // Connect to Worker 3
            Registry registry3 =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2003
                    );

            WorkerService worker3 =
                    (WorkerService)
                            registry3.lookup(
                                    "Worker3"
                            );


            // -----------------------------------
            // ELECT A COORDINATOR FIRST
            // -----------------------------------

            System.out.println(
                    "Starting leader election..."
            );

            worker1.startElection();


            int coordinatorId =
                    worker1.getCoordinatorId();


            System.out.println(
                    "Coordinator = Worker "
                            + coordinatorId
            );


            WorkerService coordinator;


            if (coordinatorId == 1) {

                coordinator = worker1;

            } else if (coordinatorId == 2) {

                coordinator = worker2;

            } else if (coordinatorId == 3) {

                coordinator = worker3;

            } else {

                throw new Exception(
                        "No valid coordinator found."
                );
            }


            // ===================================
            // TEST 1 - MAX
            // ===================================

            JobRequest maxJob =
                    new JobRequest(
                            JobType.MAX,
                            Arrays.asList(
                                    12,
                                    45,
                                    7,
                                    99,
                                    23,
                                    56
                            )
                    );


            JobResult maxResult =
                    coordinator.submitJob(
                            maxJob
                    );


            System.out.println();
            System.out.println(
                    "MAX RESULT = "
                            + maxResult.getResult()
            );


            // ===================================
            // TEST 2 - PRIMESUM
            // ===================================

            JobRequest primeSumJob =
                    new JobRequest(
                            JobType.PRIMESUM,
                            1,
                            10
                    );


            JobResult primeSumResult =
                    coordinator.submitJob(
                            primeSumJob
                    );


            System.out.println(
                    "PRIMESUM RESULT = "
                            + primeSumResult.getResult()
            );


            // ===================================
            // TEST 3 - PRIMECOUNT
            // ===================================

            JobRequest primeCountJob =
                    new JobRequest(
                            JobType.PRIMECOUNT,
                            Arrays.asList(
                                    2,
                                    4,
                                    5,
                                    7,
                                    9,
                                    11,
                                    12
                            )
                    );


            JobResult primeCountResult =
                    coordinator.submitJob(
                            primeCountJob
                    );


            System.out.println(
                    "PRIMECOUNT RESULT = "
                            + primeCountResult.getResult()
            );


            // ===================================
            // CHECK RESULTS
            // ===================================

            boolean maxPassed =
                    maxResult.getResult() == 99;

            boolean primeSumPassed =
                    primeSumResult.getResult() == 17;

            boolean primeCountPassed =
                    primeCountResult.getResult() == 4;


            System.out.println();
            System.out.println(
                    "======================================"
            );

            if (
                    maxPassed
                            &&
                            primeSumPassed
                            &&
                            primeCountPassed
            ) {

                System.out.println(
                        "ALL DISTRIBUTED JOB TESTS PASSED."
                );

            } else {

                System.out.println(
                        "JOB PROCESSING TEST FAILED."
                );
            }

            System.out.println(
                    "======================================"
            );


        } catch (Exception e) {

            System.err.println(
                    "Distributed job test failed."
            );

            e.printStackTrace();
        }
    }
}