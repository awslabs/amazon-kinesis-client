/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.leases;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.checkpoint.DynamoDBCheckpointer;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;

@RunWith(MockitoJUnitRunner.class)
public class KinesisClientLibLeaseCoordinatorTest {
    private static final String SHARD_ID = "shardId-test";
    private static final String WORK_ID = "workId-test";
    private static final long TEST_LONG = 1000L;
    private static final ExtendedSequenceNumber TEST_CHKPT = new ExtendedSequenceNumber("string-test");
    private static final UUID TEST_UUID = UUID.randomUUID();

    @Mock
    private LeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private LeaseCoordinator<KinesisClientLease> leaseCoordinator;
    @Mock
    private IMetricsFactory metricsFactory;

    private DynamoDBCheckpointer dynamoDBCheckpointer;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpLeaseCoordinator() throws ProvisionedThroughputException, DependencyException {
        dynamoDBCheckpointer = new DynamoDBCheckpointer(leaseCoordinator, leaseManager, metricsFactory);
    }

    @Test(expected = ShutdownException.class)
    public void testSetCheckpointWithUnownedShardId() throws KinesisClientLibException, DependencyException, InvalidStateException, ProvisionedThroughputException {
        final KinesisClientLease lease = new KinesisClientLease();
        when(leaseManager.getLease(eq(SHARD_ID))).thenReturn(lease);
        when(leaseCoordinator.updateLease(eq(lease), eq(TEST_UUID))).thenReturn(false);

        dynamoDBCheckpointer.setCheckpoint(SHARD_ID, TEST_CHKPT, TEST_UUID.toString());

        verify(leaseManager).getLease(eq(SHARD_ID));
        verify(leaseCoordinator).updateLease(eq(lease), eq(TEST_UUID));
    }

//    @Test(expected = DependencyException.class)
//    public void testWaitLeaseTableTimeout()
//        throws DependencyException, ProvisionedThroughputException, IllegalStateException {
//         Set mock lease manager to return false in waiting
//        doReturn(false).when(leaseManager).waitUntilLeaseTableExists(anyLong(), anyLong());
//        leaseCoordinator.initialize();
//    }
}
