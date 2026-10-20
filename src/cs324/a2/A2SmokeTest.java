package cs324.a2;

import cs324.client.GrpcClientConnection;
import cs324.common.JobRequest;
import cs324.common.JobType;
import cs324.grpc.JobProgress;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Start Bootstrap and 3-10 gRPC workers before running this test. */
public class A2SmokeTest {

    private static void check(JobRequest job, BigInteger expected) throws Exception {
        AtomicReference<BigInteger> answer = new AtomicReference<>();
        AtomicInteger maxProgress = new AtomicInteger(0);
        AtomicInteger updates = new AtomicInteger();

        GrpcClientConnection.stream(job, update -> {
            updates.incrementAndGet();
            int pct = update.getPercent();
            if (pct < 0 || pct > 100 || update.getCompleted() > update.getTotal()) {
                throw new AssertionError("Invalid progress for " + job.getJobType());
            }
            maxProgress.accumulateAndGet(pct, Math::max);
            System.out.println(job.getJobType() + ": " + pct + "% ("
                    + update.getCompleted() + "/" + update.getTotal() + ")");
            if (update.getFinished()) {
                if (pct != 100) throw new AssertionError("Finished before 100%");
                answer.set(new BigInteger(update.getResult()));
            }
        });

        if (!expected.equals(answer.get())) {
            throw new AssertionError(job.getJobType() + " expected " + expected
                    + " but got " + answer.get());
        }
        if (updates.get() < 1 || maxProgress.get() != 100) {
            throw new AssertionError("No complete progress events received");
        }
        System.out.println("PASS " + job.getJobType() + " = " + answer.get());
    }

    public static void main(String[] args) throws Exception {
        List<BigInteger> huge = Arrays.asList(
                new BigInteger("1000000000000000000000000000000000000"),
                new BigInteger("9999999999999999999999999999999999999"),
                BigInteger.valueOf(50));
        check(new JobRequest(JobType.MAX, huge), huge.get(1));
        check(new JobRequest(JobType.PRIMESUM, 2, 10), BigInteger.valueOf(17));
        check(new JobRequest(JobType.PRIMECOUNT, Arrays.asList(2, 3, 5, 7, 8, 9)),
                BigInteger.valueOf(4));
        System.out.println("ALL A2 SMOKE TESTS PASSED");
    }
}
