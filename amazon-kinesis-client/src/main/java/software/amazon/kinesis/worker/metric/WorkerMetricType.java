package software.amazon.kinesis.worker.metric;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum WorkerMetricType {
    CPU("C"),
    MEMORY("M"),
    NETWORK_IN("NI"),
    NETWORK_OUT("NO"),
    THROUGHPUT("T");

    @Getter
    private final String shortName;
}
