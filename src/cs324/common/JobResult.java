package cs324.common;

import java.io.Serializable;

public class JobResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final long result;
    private final int coordinatorId;

    public JobResult(
            String jobId,
            long result,
            int coordinatorId) {

        this.jobId =
                jobId;

        this.result =
                result;

        this.coordinatorId =
                coordinatorId;
    }


    public String getJobId() {

        return jobId;
    }


    public long getResult() {

        return result;
    }


    public int getCoordinatorId() {

        return coordinatorId;
    }


    @Override
    public String toString() {

        return "JobResult{" +
                "jobId='" + jobId + '\'' +
                ", result=" + result +
                ", coordinatorId=" +
                coordinatorId +
                '}';
    }
}