/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.kinesis.leases.dynamodb;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseIntegrationBillingModePayPerRequestTest;
import software.amazon.kinesis.leases.LeaseRenewer;
import software.amazon.kinesis.leases.LeaseStatsRecorder;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseRenewerBillingModePayPerRequestIntegrationTest
        extends LeaseIntegrationBillingModePayPerRequestTest {
    private static final String TEST_METRIC = "TestOperation";

    // This test case's leases last 2 seconds
    private static final long LEASE_DURATION_MILLIS = 2000L;

    @Mock
    private LeaseStatsRecorder leaseStatsRecorder;

    private static final MetricsFactory NULL_METRICS_FACTORY = new NullMetricsFactory();

    private LeaseRenewer renewer;

    @Before
    public void setup() {
        renewer = new DynamoDBLeaseRenewer(
                leaseRefresher,
                "foo",
                LEASE_DURATION_MILLIS,
                Executors.newCachedThreadPool(),
                NULL_METRICS_FACTORY,
                leaseStatsRecorder,
                lease -> {});
    }

    @Test
    public void testSimpleRenew() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");
    }

    @Test
    public void testLeaseLoss() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").withLease("2", "foo").build();

        builder.addLeasesToRenew(renewer, "1", "2");
        Lease renewedLease = builder.renewMutateAssert(renewer, "1", "2").get("2");

        // lose lease 2
        leaseRefresher.takeLease(renewedLease, "bar");

        builder.renewMutateAssert(renewer, "1");
    }

    @Test
    public void testClear() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();
        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        renewer.clearCurrentlyHeldLeases();
        builder.renewMutateAssert(renewer);
    }

    @Test
    public void testGetCurrentlyHeldLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();
        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        // this should be a copy that doesn't get updated
        Lease lease = renewer.getCurrentlyHeldLease("1");
        assertThat(lease.leaseCounter(), equalTo(1L));

        // do one renewal and make sure the old copy doesn't get updated
        builder.renewMutateAssert(renewer, "1");

        assertThat(lease.leaseCounter(), equalTo(1L));
    }

    @Test
    public void testGetCurrentlyHeldLeases() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").withLease("2", "foo").build();
        builder.addLeasesToRenew(renewer, "1", "2");
        Lease lease2 = builder.renewMutateAssert(renewer, "1", "2").get("2");

        // This should be a copy that doesn't get updated
        Map<String, Lease> heldLeases = renewer.getCurrentlyHeldLeases();
        assertThat(heldLeases.size(), equalTo(2));
        assertThat(heldLeases.get("1").leaseCounter(), equalTo(1L));
        assertThat(heldLeases.get("2").leaseCounter(), equalTo(1L));

        // lose lease 2
        leaseRefresher.takeLease(lease2, "bar");

        // Do another renewal and make sure the copy doesn't change
        builder.renewMutateAssert(renewer, "1");

        assertThat(heldLeases.size(), equalTo(2));
        assertThat(heldLeases.get("1").leaseCounter(), equalTo(1L));
        assertThat(heldLeases.get("2").leaseCounter(), equalTo(1L));
    }

    @Test
    public void testUpdateLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        Lease expected = renewer.getCurrentlyHeldLease("1");
        expected.checkpoint(new ExtendedSequenceNumber("new checkpoint"));
        assertThat(
                renewer.updateLease(expected, expected.concurrencyToken(), TEST_METRIC, expected.leaseKey()),
                equalTo(true));

        // Assert that the counter and data have changed immediately after the update...
        Lease actual = renewer.getCurrentlyHeldLease("1");
        expected.leaseCounter(expected.leaseCounter() + 1);
        assertThat(actual, equalTo(expected));

        // ...and after another round of renewal
        renewer.renewLeases();
        actual = renewer.getCurrentlyHeldLease("1");
        expected.leaseCounter(expected.leaseCounter() + 1);
        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testUpdateLostLease() throws Exception {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        Lease lease = renewer.getCurrentlyHeldLease("1");

        // cause lease loss such that the renewer doesn't realize he's lost the lease when update is called
        leaseRefresher.renewLease(lease);

        // renewer still thinks he has the lease
        assertThat(renewer.getCurrentlyHeldLease("1"), notNullValue());
        lease.checkpoint(new ExtendedSequenceNumber("new checkpoint"));

        // update fails
        assertThat(renewer.updateLease(lease, lease.concurrencyToken(), TEST_METRIC, null), equalTo(false));
        // renewer no longer thinks he has the lease
        assertThat(renewer.getCurrentlyHeldLease("1"), nullValue());
    }

    @Test
    public void testUpdateOldLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        Lease lease = renewer.getCurrentlyHeldLease("1");

        // cause lease loss such that the renewer knows the lease has been lost when update is called
        leaseRefresher.takeLease(lease, "bar");
        builder.renewMutateAssert(renewer);

        lease.checkpoint(new ExtendedSequenceNumber("new checkpoint"));
        assertThat(renewer.updateLease(lease, lease.concurrencyToken(), TEST_METRIC, lease.leaseKey()), equalTo(false));
    }

    @Test
    public void testUpdateRegainedLease() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        Lease lease = renewer.getCurrentlyHeldLease("1");

        // cause lease loss such that the renewer knows the lease has been lost when update is called
        leaseRefresher.takeLease(lease, "bar");
        builder.renewMutateAssert(renewer);

        // regain the lease
        builder.addLeasesToRenew(renewer, "1");

        lease.checkpoint(new ExtendedSequenceNumber("new checkpoint"));
        assertThat(renewer.updateLease(lease, lease.concurrencyToken(), TEST_METRIC, lease.leaseKey()), equalTo(false));
    }

    @Test
    public void testIgnoreNoRenewalTimestamp() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        Lease lease = builder.withLease("1", "foo").build().get("1");
        lease.lastCounterIncrementNanos(null);

        renewer.addLeasesToRenew(Collections.singleton(lease));

        assertThat(renewer.getCurrentlyHeldLeases().size(), equalTo(0));
    }

    @Test
    public void testLeaseTimeout() throws LeasingException, InterruptedException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "foo").build();

        builder.addLeasesToRenew(renewer, "1");
        builder.renewMutateAssert(renewer, "1");

        // TODO: Worth eliminating this sleep using the same pattern we used on LeaseTaker?
        Thread.sleep(LEASE_DURATION_MILLIS); // Wait for the lease to timeout

        assertThat(renewer.getCurrentlyHeldLeases().size(), equalTo(0));
    }

    @Test
    public void testInitialize() throws LeasingException {
        final String shardId = "shd-0-0";
        final String owner = "foo:8000";

        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        builder.withLease(shardId, owner);
        Map<String, Lease> leases = builder.build();
        DynamoDBLeaseRenewer renewer = new DynamoDBLeaseRenewer(
                leaseRefresher,
                owner,
                30000L,
                Executors.newCachedThreadPool(),
                NULL_METRICS_FACTORY,
                leaseStatsRecorder,
                lease -> {});
        renewer.initialize();
        Map<String, Lease> heldLeases = renewer.getCurrentlyHeldLeases();
        assertThat(heldLeases.size(), equalTo(leases.size()));
        assertThat(heldLeases.keySet(), equalTo(leases.keySet()));
    }

    @Test
    public void testInitializeBillingMode() throws LeasingException {
        final String shardId = "shd-0-0";
        final String owner = "foo:8000";
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);
        builder.withLease(shardId, owner);
        Map<String, Lease> leases = builder.build();
        DynamoDBLeaseRenewer renewer = new DynamoDBLeaseRenewer(
                leaseRefresher,
                owner,
                30000L,
                Executors.newCachedThreadPool(),
                NULL_METRICS_FACTORY,
                leaseStatsRecorder,
                lease -> {});
        renewer.initialize();
        Map<String, Lease> heldLeases = renewer.getCurrentlyHeldLeases();
        assertThat(heldLeases.size(), equalTo(leases.size()));
        assertThat(heldLeases.keySet(), equalTo(leases.keySet()));
    }
}
