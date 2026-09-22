package cs324.bootstrap;

import cs324.common.WorkerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface BootstrapService extends Remote {

    WorkerInfo registerWorker(WorkerInfo worker)
            throws RemoteException;

    void unregisterWorker(int workerId)
            throws RemoteException;

    List<WorkerInfo> getActiveWorkers()
            throws RemoteException;

    WorkerInfo getRandomActiveWorker(int excludeWorkerId)
            throws RemoteException;
}