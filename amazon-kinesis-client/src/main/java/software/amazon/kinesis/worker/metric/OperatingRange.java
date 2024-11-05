package software.amazon.kinesis.worker.metric;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OperatingRange {

    /**
     * Max utilization percentage allowed for the workerMetrics.
     */
    private final int maxUtilization;

    private OperatingRange(final int maxUtilization) {
        Preconditions.checkArgument(!(maxUtilization < 0 || maxUtilization > 100), "Invalid maxUtilization value");
        this.maxUtilization = maxUtilization;
    }
}
