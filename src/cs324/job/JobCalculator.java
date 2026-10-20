
package cs324.job;

import cs324.common.JobRequest;
import cs324.common.JobType;

import java.math.BigInteger;
import java.util.List;

public class JobCalculator {

    private JobCalculator() {
        // Prevent creating JobCalculator objects
    }

    public static BigInteger calculate(JobRequest job) {

        if (job == null) {
            throw new IllegalArgumentException("Job cannot be null.");
        }

        JobType type = job.getJobType();

        switch (type) {

            case MAX:
                return calculateMax(job.getBigNumbers());

            case PRIMECOUNT:
                return calculatePrimeCount(job.getBigNumbers());

            case PRIMESUM:
                return calculatePrimeSum(
                        job.getBigStart(),
                        job.getBigEnd()
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

    private static BigInteger calculateMax(
            List<BigInteger> numbers) {

        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "MAX requires at least one number."
            );
        }

        BigInteger maximum = numbers.get(0);

        for (BigInteger number : numbers) {

            if (number.compareTo(maximum) > 0) {
                maximum = number;
            }
        }

        return maximum;
    }


    // ==========================================
    // PRIMECOUNT
    // ==========================================

    private static BigInteger calculatePrimeCount(
            List<BigInteger> numbers) {

        if (numbers == null) {
            throw new IllegalArgumentException(
                    "Number list cannot be null."
            );
        }

        BigInteger count = BigInteger.ZERO;

        for (BigInteger number : numbers) {

            if (isPrime(number)) {
                count = count.add(BigInteger.ONE);
            }
        }

        return count;
    }


    // ==========================================
    // PRIMESUM
    // ==========================================

    private static BigInteger calculatePrimeSum(
            BigInteger start,
            BigInteger end) {

        if (start.compareTo(end) > 0) {
            throw new IllegalArgumentException(
                    "Start cannot be greater than end."
            );
        }

        BigInteger sum = BigInteger.ZERO;

        BigInteger number = start.max(BigInteger.TWO);

        while (number.compareTo(end) <= 0) {

            if (isPrime(number)) {
                sum = sum.add(number);
            }

            number = number.add(BigInteger.ONE);
        }

        return sum;
    }


    // ==========================================
    // PRIME CHECK
    // ==========================================

    private static boolean isPrime(BigInteger number) {

        if (number.compareTo(BigInteger.TWO) < 0) {
            return false;
        }

        return number.isProbablePrime(100);
    }
}
