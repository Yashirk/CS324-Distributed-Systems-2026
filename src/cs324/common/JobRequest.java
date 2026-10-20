
package cs324.common;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JobRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final JobType jobType;

    private final List<BigInteger> numbers;

    private final BigInteger start;
    private final BigInteger end;

    // Constructor for MAX and PRIMECOUNT
    public JobRequest(
            JobType jobType,
            List<? extends Number> numbers) {

        this.jobId = UUID.randomUUID().toString();
        this.jobType = jobType;

        this.numbers = new ArrayList<>();

        for (Number number : numbers) {
            this.numbers.add(
                    new BigInteger(number.toString())
            );
        }

        this.start = BigInteger.ZERO;
        this.end = BigInteger.ZERO;
    }

    // Constructor for PRIMESUM
    public JobRequest(
            JobType jobType,
            BigInteger start,
            BigInteger end) {

        this.jobId = UUID.randomUUID().toString();
        this.jobType = jobType;

        this.numbers = new ArrayList<>();

        this.start = start;
        this.end = end;
    }

    // Keep the original constructor for existing code
    public JobRequest(
            JobType jobType,
            int start,
            int end) {

        this(
                jobType,
                BigInteger.valueOf(start),
                BigInteger.valueOf(end)
        );
    }

    public String getJobId() {
        return jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    // BigInteger values for the updated calculation classes
    public List<BigInteger> getBigNumbers() {
        return new ArrayList<>(numbers);
    }

    public BigInteger getBigStart() {
        return start;
    }

    public BigInteger getBigEnd() {
        return end;
    }

    // Original getters for compatibility with existing classes
    public List<Integer> getNumbers() {

        List<Integer> result = new ArrayList<>();

        for (BigInteger number : numbers) {
            result.add(number.intValueExact());
        }

        return result;
    }

    public int getStart() {
        return start.intValueExact();
    }

    public int getEnd() {
        return end.intValueExact();
    }

    @Override
    public String toString() {

        return "JobRequest{" +
                "jobId='" + jobId + '\'' +
                ", jobType=" + jobType +
                ", numbers=" + numbers +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
