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

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.checkpoint.dynamodb.DynamoDBCheckpointer;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBCheckpointerTest {
    private static final String SHARD_ID = "shardId-test";
    private static final ExtendedSequenceNumber TEST_CHKPT = new ExtendedSequenceNumber("string-test");
    private static final UUID TEST_UUID = UUID.randomUUID();
    private static final String OPERATION = "TestOperation";

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    private DynamoDBCheckpointer dynamoDBCheckpointer;

    @Before
    public void setup() {
        dynamoDBCheckpointer = new DynamoDBCheckpointer(leaseCoordinator, leaseRefresher);
        dynamoDBCheckpointer.operation(OPERATION);
    }

    @Test(expected = ShutdownException.class)
    public void testSetCheckpointWithUnownedShardId()
            throws KinesisClientLibException, DependencyException, InvalidStateException,
                    ProvisionedThroughputException {
        final Lease lease = new Lease();
        when(leaseCoordinator.getCurrentlyHeldLease(eq(SHARD_ID))).thenReturn(lease);
        when(leaseCoordinator.updateLease(eq(lease), eq(TEST_UUID), eq(OPERATION), eq(SHARD_ID)))
                .thenReturn(false);
        try {
            dynamoDBCheckpointer.setCheckpoint(SHARD_ID, TEST_CHKPT, TEST_UUID.toString());
        } finally {
            verify(leaseCoordinator).getCurrentlyHeldLease(eq(SHARD_ID));
            verify(leaseCoordinator).updateLease(eq(lease), eq(TEST_UUID), eq(OPERATION), eq(SHARD_ID));
        }
    }

    //    @Test(expected = DependencyException.class)
    //    public void testWaitLeaseTableTimeout()
    //        throws DependencyException, ProvisionedThroughputException, IllegalStateException {
    //         Set mock lease manager to return false in waiting
    //        doReturn(false).when(leaseRefresher).waitUntilLeaseTableExists(anyLong(), anyLong());
    //        leaseCoordinator.initialize();
    //    }
}
