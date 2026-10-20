
package cs324.worker;

import cs324.bootstrap.BootstrapService;
import cs324.common.WorkerInfo;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class ElectionTest {

    public static void main(String[] args) {

        try {
            Registry registry =
                    LocateRegistry.getRegistry("localhost", 1099);

            BootstrapService bootstrap =
                    (BootstrapService) registry.lookup("BootstrapService");

            if (!bootstrap.isElectionReady()) {
                System.out.println("Waiting for all workers to connect.");
                return;
            }

            List<WorkerInfo> workers = bootstrap.getActiveWorkers();

            if (workers.isEmpty()) {
                System.out.println("No workers available.");
                return;
            }

            WorkerInfo firstWorker = workers.get(0);

            Registry workerRegistry = LocateRegistry.getRegistry(
                    firstWorker.getHost(),
                    firstWorker.getRegistryPort()
            );

            WorkerService worker = (WorkerService)
                    workerRegistry.lookup(firstWorker.getServiceName());

            int coordinator = worker.getCoordinatorId();

            if (coordinator == -1) {
                worker.startElection();
                coordinator = worker.getCoordinatorId();
            }

            for (WorkerInfo info : workers) {

                Registry remoteRegistry = LocateRegistry.getRegistry(
                        info.getHost(),
                        info.getRegistryPort()
                );

                WorkerService remoteWorker = (WorkerService)
                        remoteRegistry.lookup(info.getServiceName());

                if (remoteWorker.getCoordinatorId() != coordinator) {
                    System.out.println(
                            "Coordinator election not completed."
                    );
                    return;
                }
            }

            if (coordinator != -1) {
                System.out.println(
                        "ELECTED COORDINATOR: Worker " + coordinator
                );
            } else {
                System.out.println("Coordinator election not completed.");
            }

        } catch (Exception e) {
            System.out.println(
                    "Election test failed: " + e.getMessage()
            );
        }
    }
}
