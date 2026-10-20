
package cs324.job;

import cs324.common.JobRequest;
import cs324.common.JobType;

import java.math.BigInteger;
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


    // ==========================================
    // SPLIT NUMBER LIST
    // ==========================================

    private static List<JobRequest> splitNumberList(
            JobRequest job,
            int workerCount) {

        List<BigInteger> numbers = job.getBigNumbers();

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Number list cannot be empty."
            );
        }

        int parts = Math.min(
                workerCount,
                numbers.size()
        );

        List<JobRequest> jobs = new ArrayList<>();

        int baseSize = numbers.size() / parts;
        int remainder = numbers.size() % parts;

        int index = 0;

        for (int i = 0; i < parts; i++) {

            int size = baseSize + (i < remainder ? 1 : 0);

            List<BigInteger> chunk = new ArrayList<>(
                    numbers.subList(index, index + size)
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


    // ==========================================
    // SPLIT PRIME SUM
    // ==========================================

    private static List<JobRequest> splitPrimeSum(
            JobRequest job,
            int workerCount) {

        BigInteger start = job.getBigStart();
        BigInteger end = job.getBigEnd();

        if (start.compareTo(end) > 0) {
            throw new IllegalArgumentException(
                    "Start cannot be greater than end."
            );
        }

        BigInteger totalNumbers = end.subtract(start)
                .add(BigInteger.ONE);

        int parts = totalNumbers.min(
                BigInteger.valueOf(workerCount)
        ).intValueExact();

        BigInteger[] division = totalNumbers.divideAndRemainder(
                BigInteger.valueOf(parts)
        );

        BigInteger baseSize = division[0];
        int remainder = division[1].intValueExact();

        List<JobRequest> jobs = new ArrayList<>();

        BigInteger currentStart = start;

        for (int i = 0; i < parts; i++) {

            BigInteger size = baseSize;

            if (i < remainder) {
                size = size.add(BigInteger.ONE);
            }

            BigInteger currentEnd = currentStart
                    .add(size)
                    .subtract(BigInteger.ONE);

            jobs.add(
                    new JobRequest(
                            JobType.PRIMESUM,
                            currentStart,
                            currentEnd
                    )
            );

            currentStart = currentEnd.add(BigInteger.ONE);
        }

        return jobs;
    }


    // ==========================================
    // COMBINE RESULTS
    // ==========================================

    public static BigInteger combineResults(
            JobType type,
            List<BigInteger> results) {

        if (results == null || results.isEmpty()) {
            throw new IllegalArgumentException(
                    "No worker results received."
            );
        }

        if (type == JobType.MAX) {

            BigInteger maximum = results.get(0);

            for (BigInteger result : results) {

                if (result.compareTo(maximum) > 0) {
                    maximum = result;
                }
            }

            return maximum;
        }

        BigInteger total = BigInteger.ZERO;

        for (BigInteger result : results) {
            total = total.add(result);
        }

        return total;
    }
}
