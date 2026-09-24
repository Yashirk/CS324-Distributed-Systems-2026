package cs324.worker;

import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.JobType;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class CoordinatorTermTest {

    public static void main(String[] args) {

        try {

            Registry r1 =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2001
                    );

            Registry r2 =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2002
                    );

            Registry r3 =
                    LocateRegistry.getRegistry(
                            "localhost",
                            2003
                    );


            WorkerService worker1 =
                    (WorkerService)
                            r1.lookup("Worker1");

            WorkerService worker2 =
                    (WorkerService)
                            r2.lookup("Worker2");

            WorkerService worker3 =
                    (WorkerService)
                            r3.lookup("Worker3");


            // Start first election
            worker1.startElection();


            int originalCoordinator =
                    worker1.getCoordinatorId();


            System.out.println(
                    "Original Coordinator = Worker "
                            + originalCoordinator
            );


            WorkerService coordinator;

            if (originalCoordinator == 1) {

                coordinator = worker1;

            } else if (originalCoordinator == 2) {

                coordinator = worker2;

            } else {

                coordinator = worker3;
            }


            // Submit exactly 5 complete jobs
            for (int i = 1; i <= 5; i++) {

                JobRequest job =
                        new JobRequest(
                                JobType.MAX,
                                Arrays.asList(
                                        i,
                                        i * 10,
                                        i * 5
                                )
                        );


                JobResult result =
                        coordinator.submitJob(job);


                System.out.println(
                        "Job "
                                + i
                                + " completed. Result = "
                                + result.getResult()
                );
            }


            System.out.println();
            System.out.println(
                    "Waiting for automatic new election..."
            );


            // Give election time to propagate
            Thread.sleep(2500);


            int newCoordinator1 =
                    worker1.getCoordinatorId();

            int newCoordinator2 =
                    worker2.getCoordinatorId();

            int newCoordinator3 =
                    worker3.getCoordinatorId();


            System.out.println();
            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "AFTER 5 JOBS"
            );

            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "Worker 1 says Coordinator = Worker "
                            + newCoordinator1
            );

            System.out.println(
                    "Worker 2 says Coordinator = Worker "
                            + newCoordinator2
            );

            System.out.println(
                    "Worker 3 says Coordinator = Worker "
                            + newCoordinator3
            );


            if (
                    newCoordinator1 == newCoordinator2
                            &&
                            newCoordinator2 == newCoordinator3
                            &&
                            newCoordinator1 != originalCoordinator
            ) {

                System.out.println();
                System.out.println(
                        "COORDINATOR TERM TEST PASSED."
                );

                System.out.println(
                        "A new leader was elected after 5 jobs."
                );

            } else {

                System.out.println();
                System.out.println(
                        "COORDINATOR TERM TEST NEEDS CHECKING."
                );
            }


        } catch (Exception e) {

            System.err.println(
                    "Coordinator term test failed."
            );

            e.printStackTrace();
        }
    }
}