package cs324.job;

import cs324.common.JobRequest;
import cs324.common.JobType;

import java.util.List;

public class JobCalculator {

    private JobCalculator() {
        // Prevent creating JobCalculator objects
    }

    public static long calculate(JobRequest job) {

        if (job == null) {
            throw new IllegalArgumentException("Job cannot be null.");
        }

        JobType type = job.getJobType();

        switch (type) {

            case MAX:
                return calculateMax(job.getNumbers());

            case PRIMECOUNT:
                return calculatePrimeCount(job.getNumbers());

            case PRIMESUM:
                return calculatePrimeSum(
                        job.getStart(),
                        job.getEnd()
                );

            default:
                throw new IllegalArgumentException(
                        "Unsupported job type."
                );
        }
    }


    // ==========================================
    // MAX
    // ==========================================

    private static long calculateMax(
            List<Integer> numbers) {

        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "MAX requires at least one number."
            );
        }

        long maximum = numbers.get(0);

        for (int number : numbers) {

            if (number > maximum) {
                maximum = number;
            }
        }

        return maximum;
    }


    // ==========================================
    // PRIMECOUNT
    // ==========================================

    private static long calculatePrimeCount(
            List<Integer> numbers) {

        if (numbers == null) {
            throw new IllegalArgumentException(
                    "Number list cannot be null."
            );
        }

        long count = 0;

        for (int number : numbers) {

            if (isPrime(number)) {
                count++;
            }
        }

        return count;
    }


    // ==========================================
    // PRIMESUM
    // ==========================================

    private static long calculatePrimeSum(
            int start,
            int end) {

        if (start > end) {
            throw new IllegalArgumentException(
                    "Start cannot be greater than end."
            );
        }

        long sum = 0;

        for (int number = start;
             number <= end;
             number++) {

            if (isPrime(number)) {
                sum += number;
            }
        }

        return sum;
    }


    // ==========================================
    // PRIME CHECK
    // ==========================================

    private static boolean isPrime(
            int number) {

        if (number < 2) {
            return false;
        }

        if (number == 2) {
            return true;
        }

        if (number % 2 == 0) {
            return false;
        }

        for (int i = 3;
             (long) i * i <= number;
             i += 2) {

            if (number % i == 0) {
                return false;
            }
        }

        return true;
    }
}