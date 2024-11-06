package software.amazon.kinesis.leases;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class LeaseStatsRecorderTest {

    private LeaseStatsRecorder leaseStatsRecorder;
    private Callable<Long> mockedTimeProviderInMillis;
    private static final long TEST_RENEWER_FREQ = Duration.ofMinutes(1).toMillis();

    @BeforeEach
    void setup() {
        mockedTimeProviderInMillis = Mockito.mock(Callable.class);
        leaseStatsRecorder = new LeaseStatsRecorder(TEST_RENEWER_FREQ, mockedTimeProviderInMillis);
    }

    @Test
    void leaseStatsRecorder_sanity() throws Exception {
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1"));
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1"));
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1"));
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1"));
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1"));
        when(mockedTimeProviderInMillis.call()).thenReturn(System.currentTimeMillis() + 1);

        assertEquals(
                Math.floor(this.leaseStatsRecorder.getThroughputKBps("lease-key1")),
                85.0,
                "Incorrect throughputKbps calculated");
        // Test idempotent behavior
        assertEquals(
                Math.floor(this.leaseStatsRecorder.getThroughputKBps("lease-key1")),
                85.0,
                "Incorrect throughputKbps calculated");
    }

    @Test
    void leaseStatsRecorder_validateDecayToZero() throws Exception {
        final long currentTime = System.currentTimeMillis();
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1", currentTime, 1));
        when(mockedTimeProviderInMillis.call())
                .thenReturn(currentTime + 1)
                .thenReturn(currentTime + 1)
                .thenReturn(currentTime - Duration.ofMillis(TEST_RENEWER_FREQ).toMillis() - 5);
        for (int i = 0; i < 2000; ++i) {
            this.leaseStatsRecorder.getThroughputKBps("lease-key1");
        }

        // after decaying for long time, it eventually goes to zero after going below minimum range of double
        // the test also validates that decaying does not fail with exception if we keep decaying a value
        assertEquals(0.0D, this.leaseStatsRecorder.getThroughputKBps("lease-key1"));
    }

    @Test
    void leaseStatsRecorder_validateVeryHighThroughout() throws Exception {
        final long currentTime = System.currentTimeMillis();
        // 1000 stats recorded
        for (int i = 0; i < 1000; ++i) {
            // 1 GB of bytes per stats
            this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1", currentTime, 1024 * 1024 * 1024));
        }
        when(mockedTimeProviderInMillis.call()).thenReturn(System.currentTimeMillis() + 1);
        assertEquals(17476266D, Math.floor(this.leaseStatsRecorder.getThroughputKBps("lease-key1")));
    }

    @Test
    void leaseStatsRecorder_expiredItems_assertZeroOutput() throws Exception {
        // Insert an expired item
        this.leaseStatsRecorder.recordStats(
                generateRandomLeaseStat("lease-key1", System.currentTimeMillis() - TEST_RENEWER_FREQ - 10));
        when(mockedTimeProviderInMillis.call()).thenReturn(System.currentTimeMillis() + 1);

        assertEquals(
                this.leaseStatsRecorder.getThroughputKBps("lease-key1"),
                0.0,
                "throughputKbps is not 0 when in case where all items are expired.");
    }

    @Test
    void getThroughputKbps_noEntryPresent_assertNull() throws Exception {
        when(mockedTimeProviderInMillis.call()).thenReturn(System.currentTimeMillis());
        assertNull(
                this.leaseStatsRecorder.getThroughputKBps(UUID.randomUUID().toString()),
                "Did not return null for non existing leaseKey stats.");
    }

    @Test
    void dropLeaseStats_sanity() throws Exception {
        this.leaseStatsRecorder.recordStats(generateRandomLeaseStat("lease-key1"));
        when(mockedTimeProviderInMillis.call()).thenReturn(System.currentTimeMillis() + 1);

        assertEquals(
                Math.floor(this.leaseStatsRecorder.getThroughputKBps("lease-key1")),
                17.0,
                "Incorrect throughputKbps calculated");

        this.leaseStatsRecorder.dropLeaseStats("lease-key1");
        // after drop, no entry is present and thus validate method returns null.
        assertNull(
                this.leaseStatsRecorder.getThroughputKBps("lease-key1"),
                "LeaseStats exists even after dropping lease stats");
    }

    private static LeaseStatsRecorder.LeaseStats generateRandomLeaseStat(final String leaseKey) {
        return generateRandomLeaseStat(leaseKey, System.currentTimeMillis());
    }

    private static LeaseStatsRecorder.LeaseStats generateRandomLeaseStat(
            final String leaseKey, final long creationTimeMillis) {
        // 1 MB data
        return generateRandomLeaseStat(leaseKey, creationTimeMillis, 1024 * 1024);
    }

    private static LeaseStatsRecorder.LeaseStats generateRandomLeaseStat(
            final String leaseKey, final long creationTimeMillis, final long bytes) {
        LeaseStatsRecorder.LeaseStats leaseStats = LeaseStatsRecorder.LeaseStats.builder()
                .leaseKey(leaseKey)
                .bytes(bytes)
                .creationTimeMillis(creationTimeMillis)
                .build();
        return leaseStats;
    }
}
