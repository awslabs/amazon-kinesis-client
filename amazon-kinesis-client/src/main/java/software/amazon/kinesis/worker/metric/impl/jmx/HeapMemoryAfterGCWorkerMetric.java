package software.amazon.kinesis.worker.metric.impl.jmx;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.worker.metric.OperatingRange;
import software.amazon.kinesis.worker.metric.WorkerMetric;
import software.amazon.kinesis.worker.metric.WorkerMetricType;

/**
 * Memory WorkerMetricStats that reads the heap memory after GC. The way memory usage is calculated that, all the
 * available memory pools are read except Eden (as this is allocation buffer) and used memory and total memory is
 * computed.
 * Then percentage is computed by dividing used memory by total memory.
 *
 */
@RequiredArgsConstructor
public class HeapMemoryAfterGCWorkerMetric implements WorkerMetric {

    private static final WorkerMetricType MEMORY_WORKER_METRICS_TYPE = WorkerMetricType.MEMORY;

    private final OperatingRange operatingRange;

    private Set<ObjectName> garbageCollectorMxBeans;
    private Set<String> memoryPoolNames;

    @Override
    public String getShortName() {
        return MEMORY_WORKER_METRICS_TYPE.getShortName();
    }

    @Override
    public WorkerMetricValue capture() {
        return WorkerMetricValue.builder()
                .value(getAfterGCMemoryUsage(ManagementFactory.getPlatformMBeanServer()))
                .build();
    }

    private double getAfterGCMemoryUsage(final MBeanServerConnection connection) {
        try {
            if (garbageCollectorMxBeans == null) {
                garbageCollectorMxBeans = connection.queryNames(
                        new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*"), null);

                memoryPoolNames = new HashSet<String>();
                for (ObjectName on : garbageCollectorMxBeans) {
                    String[] poolNames = (String[]) connection.getAttribute(on, "MemoryPoolNames");
                    // A given MemoryPool may be associated with multiple GarbageCollectors,
                    // but will appear only once in memoryPoolNames
                    Collections.addAll(memoryPoolNames, poolNames);
                }
            }

            // Report on the sum of non-Eden HEAP spaces after the last gc
            Long used, max;
            long usedKb = 0, totalKb = 0;

            for (String poolName : memoryPoolNames) {
                if (!poolName.contains("Eden")) {
                    // Ignore Eden, since it's just an allocation buffer
                    ObjectName on =
                            new ObjectName(ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE + ",name=" + poolName);
                    String mt = (String) connection.getAttribute(on, "Type");
                    if (mt.equals("HEAP")) {
                        // Paranoia: ignore non-HEAP memory pools
                        CompositeDataSupport data =
                                (CompositeDataSupport) connection.getAttribute(on, "CollectionUsage");

                        used = (Long) data.get("used");
                        usedKb += used / 1024;

                        max = (Long) data.get("max");
                        // max can be undefined (-1)
                        // http://docs.oracle.com/javase/7/docs/api/java/lang/management/MemoryUsage.html
                        totalKb += max == -1 ? 0 : max / 1024;
                    }
                }
            }

            if (totalKb <= 0) {
                throw new IllegalArgumentException("Total memory value for JVM is greater than zero");
            }

            return 100.0 * (double) usedKb / (double) totalKb;
        } catch (final Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e;
            }
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public OperatingRange getOperatingRange() {
        return operatingRange;
    }

    @Override
    public WorkerMetricType getWorkerMetricType() {
        return MEMORY_WORKER_METRICS_TYPE;
    }
}
