package cs324.common;

import java.io.Serializable;

public class WorkerInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final String host;
    private final int registryPort;
    private final String serviceName;

    public WorkerInfo(int workerId, String host, int registryPort, String serviceName) {
        this.workerId = workerId;
        this.host = host;
        this.registryPort = registryPort;
        this.serviceName = serviceName;
    }

    public int getWorkerId() {
        return workerId;
    }

    public String getHost() {
        return host;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String toString() {
        return "WorkerInfo{" +
                "workerId=" + workerId +
                ", host='" + host + '\'' +
                ", registryPort=" + registryPort +
                ", serviceName='" + serviceName + '\'' +
                '}';
    }
}
