package cs324.client;

import cs324.bootstrap.BootstrapService;
import cs324.common.WorkerInfo;
import cs324.worker.WorkerService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class ClientConnection {

    private ClientConnection() {
    }

    public static WorkerService findCoordinator()
            throws Exception {

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

        List<WorkerInfo> workers =
                bootstrap.getActiveWorkers();

        if (workers == null || workers.isEmpty()) {
            throw new Exception(
                    "No active workers are available."
            );
        }

        int coordinatorId = -1;

        /*
         * Ask active workers who the current
         * Coordinator is.
         */
        for (WorkerInfo info : workers) {

            try {

                WorkerService worker =
                        connectToWorker(info);

                int reportedCoordinator =
                        worker.getCoordinatorId();

                if (reportedCoordinator > 0) {

                    coordinatorId =
                            reportedCoordinator;

                    break;
                }

            } catch (Exception ignored) {
            }
        }

        if (coordinatorId == -1) {

            throw new Exception(
                    "No Coordinator is currently active."
            );
        }

        /*
         * Find the elected Coordinator's
         * WorkerInfo.
         */
        for (WorkerInfo info : workers) {

            if (info.getWorkerId()
                    == coordinatorId) {

                return connectToWorker(
                        info
                );
            }
        }

        throw new Exception(
                "Coordinator Worker "
                        + coordinatorId
                        + " could not be contacted."
        );
    }


    private static WorkerService connectToWorker(
            WorkerInfo info)
            throws Exception {

        Registry registry =
                LocateRegistry.getRegistry(
                        info.getHost(),
                        info.getRegistryPort()
                );

        return (WorkerService)
                registry.lookup(
                        info.getServiceName()
                );
    }
}