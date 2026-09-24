package cs324.common;

import java.io.Serializable;

public class ElectionCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final int jac;

    public ElectionCandidate(int workerId, int jac) {
        this.workerId = workerId;
        this.jac = jac;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac;
    }

    @Override
    public String toString() {
        return "Worker " + workerId + " [JAC=" + jac + "]";
    }
}