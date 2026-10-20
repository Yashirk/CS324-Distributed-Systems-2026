package cs324.client;

import cs324.common.JobRequest;
import cs324.common.JobType;
import cs324.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Finds the current elected coordinator and resumes the same job after a disconnect. */
public class GrpcClientConnection {
    private GrpcClientConnection() { }

    private static JobSpec convert(JobRequest job) {
        JobSpec.Builder spec = JobSpec.newBuilder().setJobId(job.getJobId())
                .setJobType(job.getJobType().name());
        if (job.getJobType() == JobType.PRIMESUM) {
            spec.setStart(job.getBigStart().toString()).setEnd(job.getBigEnd().toString());
        } else {
            for (BigInteger n : job.getBigNumbers()) spec.addNumbers(n.toString());
        }
        return spec.build();
    }

    public static void stream(JobRequest job, Consumer<JobProgress> progress) throws Exception {
        ManagedChannel bootstrapChannel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext().build();
        BootstrapServiceGrpc.BootstrapServiceBlockingStub registry =
                BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
        JobSpec request = convert(job); // Reused on every retry: same immutable job ID.
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ManagedChannel workerChannel = null;
                try {
                    List<WorkerDetails> workers = registry.withDeadlineAfter(3, TimeUnit.SECONDS)
                            .getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
                    if (workers.isEmpty()) throw new IllegalStateException("No workers online");
                    int coordinatorId = -1;
                    for (WorkerDetails w : workers) {
                        ManagedChannel temp = ManagedChannelBuilder
                                .forAddress(w.getHost(), w.getPort()).usePlaintext().build();
                        try {
                            coordinatorId = WorkerClusterGrpc.newBlockingStub(temp)
                                    .withDeadlineAfter(2, TimeUnit.SECONDS)
                                    .currentLeader(Empty.getDefaultInstance()).getWorkerId();
                            if (coordinatorId > 0) break;
                        } catch (Exception ignored) { }
                        finally { temp.shutdownNow(); }
                    }
                    WorkerDetails coordinator = null;
                    for (WorkerDetails w : workers) {
                        if (w.getWorkerId() == coordinatorId) coordinator = w;
                    }
                    if (coordinator == null) throw new IllegalStateException("Waiting for coordinator election");
                    workerChannel = ManagedChannelBuilder.forAddress(
                            coordinator.getHost(), coordinator.getPort()).usePlaintext().build();
                    Iterator<JobProgress> updates = WorkerClusterGrpc.newBlockingStub(workerChannel)
                            .submitJob(request);
                    while (updates.hasNext()) {
                        JobProgress update = updates.next();
                        progress.accept(update);
                        if (update.getFinished()) return;
                    }
                    throw new IllegalStateException("Progress stream closed before completion");
                } catch (Exception e) {
                    // If the coordinator failed, the new one recovers replicated task state.
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                    Thread.sleep(1200);
                } finally {
                    if (workerChannel != null) workerChannel.shutdownNow();
                }
            }
            throw new InterruptedException("Client job interrupted");
        } finally {
            bootstrapChannel.shutdownNow();
        }
    }
}
