package cs324.worker;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ElectionTest {

    public static void main(String[] args) {

        try {

            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "CS324 LEADER ELECTION TEST"
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


            System.out.println(
                    "Connected to Worker 1."
            );

            System.out.println(
                    "Starting leader election..."
            );


            // Tell Worker 1 to start an election
            worker1.startElection();


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


            System.out.println();
            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "ELECTION RESULT"
            );

            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "Worker 1 says Coordinator = Worker "
                            + worker1.getCoordinatorId()
            );

            System.out.println(
                    "Worker 2 says Coordinator = Worker "
                            + worker2.getCoordinatorId()
            );

            System.out.println(
                    "Worker 3 says Coordinator = Worker "
                            + worker3.getCoordinatorId()
            );

            System.out.println(
                    "======================================"
            );


            if (
                    worker1.getCoordinatorId() == 3
                            &&
                            worker2.getCoordinatorId() == 3
                            &&
                            worker3.getCoordinatorId() == 3
            ) {

                System.out.println(
                        "LEADER ELECTION TEST PASSED."
                );

                System.out.println(
                        "Worker 3 correctly elected because all JACs are 0 and Worker 3 has the highest ID."
                );

            } else {

                System.out.println(
                        "LEADER ELECTION TEST FAILED."
                );
            }


        } catch (Exception e) {

            System.err.println(
                    "Election test failed."
            );

            e.printStackTrace();
        }
    }
}