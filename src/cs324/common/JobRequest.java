package cs324.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JobRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final JobType jobType;

    private final List<Integer> numbers;

    private final int start;
    private final int end;

    // Constructor for MAX and PRIMECOUNT
    public JobRequest(
            JobType jobType,
            List<Integer> numbers) {

        this.jobId =
                UUID.randomUUID().toString();

        this.jobType =
                jobType;

        this.numbers =
                new ArrayList<>(numbers);

        this.start = 0;
        this.end = 0;
    }


    // Constructor for PRIMESUM
    public JobRequest(
            JobType jobType,
            int start,
            int end) {

        this.jobId =
                UUID.randomUUID().toString();

        this.jobType =
                jobType;

        this.numbers =
                new ArrayList<>();

        this.start = start;
        this.end = end;
    }


    public String getJobId() {

        return jobId;
    }


    public JobType getJobType() {

        return jobType;
    }


    public List<Integer> getNumbers() {

        return new ArrayList<>(
                numbers
        );
    }


    public int getStart() {

        return start;
    }


    public int getEnd() {

        return end;
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