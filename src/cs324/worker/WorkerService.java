package cs324.worker;

import cs324.common.ElectionCandidate;
import cs324.common.JobRequest;
import cs324.common.JobResult;
import cs324.common.WorkerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface WorkerService extends Remote {

    // ==========================================
    // BASIC WORKER METHODS
    // ==========================================

    int getWorkerId() throws RemoteException;

    int getJAC() throws RemoteException;

    void incrementJAC() throws RemoteException;

    String getLeaderman() throws RemoteException;

    void addNeighbour(WorkerInfo worker) throws RemoteException;

    List<WorkerInfo> getNeighbours() throws RemoteException;


    // ==========================================
    // LEADER ELECTION METHODS
    // ==========================================

    void startElection() throws RemoteException;

    List<ElectionCandidate> processElection(
            String electionId,
            int senderId)
            throws RemoteException;

    void announceCoordinator(
            String electionId,
            int coordinatorId,
            int senderId)
            throws RemoteException;

    int getCoordinatorId() throws RemoteException;


    // ==========================================
    // DISTRIBUTED JOB PROCESSING
    // ==========================================

    /*
     * Client sends a COMPLETE job to the
     * currently elected Coordinator.
     *
     * The Coordinator will split the job
     * between available workers.
     */
    JobResult submitJob(
            JobRequest job)
            throws RemoteException;


    /*
     * Coordinator sends a PARTIAL job
     * to an individual worker.
     *
     * The worker calculates its part
     * and returns one numeric result.
     */
    long executePartialJob(
            JobRequest partialJob)
            throws RemoteException;
}