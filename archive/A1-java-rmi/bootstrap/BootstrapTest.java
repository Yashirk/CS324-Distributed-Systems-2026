package cs324.bootstrap;

import cs324.common.WorkerInfo;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class BootstrapTest {

    public static void main(String[] args) {

        try {

            Registry registry =
                    LocateRegistry.getRegistry("localhost", 1099);

            BootstrapService bootstrap =
                    (BootstrapService) registry.lookup("BootstrapService");

            System.out.println("Connected to Bootstrap successfully.");

            WorkerInfo worker1 =
                    new WorkerInfo(
                            101,
                            "localhost",
                            2001,
                            "Worker101"
                    );

            WorkerInfo worker2 =
                    new WorkerInfo(
                            102,
                            "localhost",
                            2002,
                            "Worker102"
                    );

            WorkerInfo worker3 =
                    new WorkerInfo(
                            103,
                            "localhost",
                            2003,
                            "Worker103"
                    );

            System.out.println("\nRegistering Worker 101...");
            WorkerInfo neighbour1 =
                    bootstrap.registerWorker(worker1);

            printNeighbour(neighbour1);

            System.out.println("\nRegistering Worker 102...");
            WorkerInfo neighbour2 =
                    bootstrap.registerWorker(worker2);

            printNeighbour(neighbour2);

            System.out.println("\nRegistering Worker 103...");
            WorkerInfo neighbour3 =
                    bootstrap.registerWorker(worker3);

            printNeighbour(neighbour3);

            System.out.println("\nACTIVE WORKERS:");

            List<WorkerInfo> activeWorkers =
                    bootstrap.getActiveWorkers();

            for (WorkerInfo worker : activeWorkers) {
                System.out.println(worker);
            }

            System.out.println("\nBOOTSTRAP TEST PASSED.");

        } catch (Exception e) {

            System.err.println("BOOTSTRAP TEST FAILED.");
            e.printStackTrace();
        }
    }

    private static void printNeighbour(WorkerInfo neighbour) {

        if (neighbour == null) {

            System.out.println(
                    "No neighbour available because this is the first worker."
            );

        } else {

            System.out.println(
                    "Random neighbour selected: Worker "
                            + neighbour.getWorkerId()
            );
        }
    }
}