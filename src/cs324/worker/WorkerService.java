package cs324.worker;

import cs324.common.WorkerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface WorkerService extends Remote {

    int getWorkerId() throws RemoteException;

    int getJAC() throws RemoteException;

    void incrementJAC() throws RemoteException;

    String getLeaderman() throws RemoteException;

    void addNeighbour(WorkerInfo worker) throws RemoteException;

    List<WorkerInfo> getNeighbours() throws RemoteException;
}