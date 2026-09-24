package cs324.job;

import cs324.common.JobRequest;
import cs324.common.JobType;

import java.util.ArrayList;
import java.util.List;

public class JobDistributor {

    private JobDistributor() {
        // Utility class
    }

    public static List<JobRequest> splitJob(
            JobRequest job,
            int workerCount) {

        if (workerCount <= 0) {
            throw new IllegalArgumentException(
                    "At least one worker is required."
            );
        }

        if (job.getJobType() == JobType.PRIMESUM) {
            return splitPrimeSum(job, workerCount);
        }

        return splitNumberList(job, workerCount);
    }

    private static List<JobRequest> splitNumberList(
            JobRequest job,
            int workerCount) {

        List<Integer> numbers = job.getNumbers();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty."
            );
        }

        int parts = Math.min(
                workerCount,
                numbers.size()
        );

        List<JobRequest> jobs =
                new ArrayList<>();

        int baseSize =
                numbers.size() / parts;

        int remainder =
                numbers.size() % parts;

        int index = 0;

        for (int i = 0; i < parts; i++) {

            int size =
                    baseSize
                            + (i < remainder ? 1 : 0);

            List<Integer> chunk =
                    new ArrayList<>(
                            numbers.subList(
                                    index,
                                    index + size
                            )
                    );

            jobs.add(
                    new JobRequest(
                            job.getJobType(),
                            chunk
                    )
            );

            index += size;
        }

        return jobs;
    }

    private static List<JobRequest> splitPrimeSum(
            JobRequest job,
            int workerCount) {

        int start = job.getStart();
        int end = job.getEnd();

        if (start > end) {
            throw new IllegalArgumentException(
                    "Start cannot be greater than end."
            );
        }

        int totalNumbers =
                end - start + 1;

        int parts =
                Math.min(
                        workerCount,
                        totalNumbers
                );

        int baseSize =
                totalNumbers / parts;

        int remainder =
                totalNumbers % parts;

        List<JobRequest> jobs =
                new ArrayList<>();

        int currentStart = start;

        for (int i = 0; i < parts; i++) {

            int size =
                    baseSize
                            + (i < remainder ? 1 : 0);

            int currentEnd =
                    currentStart + size - 1;

            jobs.add(
                    new JobRequest(
                            JobType.PRIMESUM,
                            currentStart,
                            currentEnd
                    )
            );

            currentStart =
                    currentEnd + 1;
        }

        return jobs;
    }

    public static long combineResults(
            JobType type,
            List<Long> results) {

        if (results.isEmpty()) {
            throw new IllegalArgumentException(
                    "No worker results received."
            );
        }

        if (type == JobType.MAX) {

            long maximum =
                    Long.MIN_VALUE;

            for (long result : results) {
                maximum =
                        Math.max(
                                maximum,
                                result
                        );
            }

            return maximum;
        }

        long total = 0;

        for (long result : results) {
            total += result;
        }

        return total;
    }
}