
package cs324.common;

import java.io.Serializable;
import java.math.BigInteger;

public class JobResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final BigInteger result;
    private final int coordinatorId;

    public JobResult(
            String jobId,
            BigInteger result,
            int coordinatorId) {

        this.jobId = jobId;
        this.result = result;
        this.coordinatorId = coordinatorId;
    }

    // Keep support for existing long values
    public JobResult(
            String jobId,
            long result,
            int coordinatorId) {

        this(
                jobId,
                BigInteger.valueOf(result),
                coordinatorId
        );
    }

    public String getJobId() {
        return jobId;
    }

    public BigInteger getResult() {
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
                ", coordinatorId=" + coordinatorId +
                '}';
    }
}
