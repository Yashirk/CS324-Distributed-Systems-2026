
package cs324.worker;

import cs324.common.ElectionCandidate;
import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.WorkerInfo;

import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;


public interface WorkerService extends Remote {

    // =====================================================
    // BASIC WORKER INFORMATION
    // =====================================================

    int getWorkerId()
            throws RemoteException;


    int getJAC()
            throws RemoteException;


    void incrementJAC()
            throws RemoteException;


    // =====================================================
    // NEIGHBOUR MANAGEMENT
    // =====================================================

    void addNeighbour(
            WorkerInfo worker)
            throws RemoteException;


    List<WorkerInfo> getNeighbours()
            throws RemoteException;


    // =====================================================
    // LEADER ELECTION
    // =====================================================

    void startElection()
            throws RemoteException;


    List<ElectionCandidate> processElection(
            String electionId,
            int senderId)
            throws RemoteException;


    void announceCoordinator(
            String electionId,
            int coordinatorId,
            int senderId)
            throws RemoteException;


    int getCoordinatorId()
            throws RemoteException;


    // =====================================================
    // DISTRIBUTED JOB PROCESSING
    // =====================================================

    JobResult submitJob(
            JobRequest job)
            throws RemoteException;


    BigInteger executePartialJob(
            JobRequest partialJob)
            throws RemoteException;
}
