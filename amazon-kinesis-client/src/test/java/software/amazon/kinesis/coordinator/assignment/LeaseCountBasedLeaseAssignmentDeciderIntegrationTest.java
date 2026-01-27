/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.coordinator.assignment;

import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.kinesis.common.DdbTableConfig;
import software.amazon.kinesis.leases.LeaseIntegrationTest;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseTaker;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.leases.dynamodb.TestHarnessBuilder;
import software.amazon.kinesis.leases.exceptions.LeasingException;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.mockito.Mockito.mock;

public class LeaseCountBasedLeaseAssignmentDeciderIntegrationTest extends LeaseIntegrationTest {

    private static final long LEASE_DURATION_MILLIS = 1000L;
    private DynamoDBLeaseTaker taker;

    @Override
    protected DynamoDBLeaseRefresher getLeaseRefresher() {
        return new DynamoDBLeaseRefresher(
                tableName,
                ddbClient,
                leaseSerializer,
                true,
                mock(TableCreatorCallback.class),
                LeaseManagementConfig.DEFAULT_REQUEST_TIMEOUT,
                new DdbTableConfig().billingMode(BillingMode.PAY_PER_REQUEST),
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_DELETION_PROTECTION_ENABLED,
                LeaseManagementConfig.DEFAULT_LEASE_TABLE_PITR_ENABLED,
                DefaultSdkAutoConstructList.getInstance());
    }

    @Before
    public void setup() {
        taker = new DynamoDBLeaseTaker(leaseRefresher, "foo", LEASE_DURATION_MILLIS, new NullMetricsFactory());
    }

    /**
     * Test 1: Basic Lease Assignment Test
     * Purpose: Verify basic lease assignment functionality
     * Scenario: 3 unassigned leases, 2 workers
     * Expected: Round-robin assignment
     */
    @Test
    public void testBasicLeaseAssignment() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", null).withLease("2", null).withLease("3", null).build();

        builder.takeMutateAssert(taker, "1", "2", "3");
    }

    /**
     * Test 2: Non-Greedy Assignment Test (replicates testNonGreedyTake)
     * Purpose: Verify balanced distribution
     * Scenario: 4 leases (3 unassigned, 1 owned by worker), 2 workers total
     * Expected: Target should be 2 leases per worker, only 2 leases taken
     */
    @Test
    public void testNonGreedyAssignment() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        for (int i = 0; i < 3; i++) {
            builder.withLease(Integer.toString(i), null);
        }
        builder.withLease("4", "bar").build();

        taker.withVeryOldLeaseDurationNanosMultiplier(5000000);
        builder.takeMutateAssert(taker, 2);
    }

    /**
     * Test 3: Lease Stealing Test (replicates testSteal)
     * Purpose: Verify stealing from most loaded worker
     * Scenario: 6 leases: 1 owned by worker1, 5 owned by worker2, 3 workers total
     * Expected: worker3 steals from worker2 (most loaded)
     */
    @Test
    public void testSteal() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "bar");
        for (int i = 2; i <= 6; i++) {
            builder.withLease(Integer.toString(i), "baz");
        }
        builder.build();

        builder.stealMutateAssert(taker, 1);
    }

    /**
     * Test 4: No Stealing When Off-By-One Test (replicates testNoStealWhenOffByOne)
     * Purpose: Verify KCL v2 stealing rules
     * Scenario: 5 leases: 2 each for worker1/worker2, 1 for worker3
     * Expected: worker3 doesn't steal (only short by 1 lease)
     */
    @Test
    public void testNoStealWhenOffByOne() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        builder.withLease("1", "bar")
                .withLease("2", "bar")
                .withLease("3", "baz")
                .withLease("4", "baz")
                .withLease("5", "foo")
                .build();

        builder.takeMutateAssert(taker);
    }

    /**
     * Test 5: Very Old Lease Priority Test (replicates testVeryOldLeaseTaker)
     * Purpose: Verify priority assignment of very old leases
     * Scenario: 3 very old unassigned leases, 2 workers
     * Expected: All old leases are assigned regardless of target calculation
     */
    @Test
    public void testVeryOldLeasePriority() throws LeasingException {
        TestHarnessBuilder builder = new TestHarnessBuilder(leaseRefresher);

        for (int i = 0; i < 3; i++) {
            builder.withLease(Integer.toString(i), null);
        }
        builder.withLease("4", "bar").build();

        builder.takeMutateAssert(taker, 3);
    }
}
