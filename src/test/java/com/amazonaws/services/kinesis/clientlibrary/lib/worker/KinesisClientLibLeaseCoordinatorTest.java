/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

public class KinesisClientLibLeaseCoordinatorTest {
    private static final String SHARD_ID = "shardId-test";
    private static final String WORK_ID = "workId-test";
    private static final long TEST_LONG = 1000L;
    private static final ExtendedSequenceNumber TEST_CHKPT = new ExtendedSequenceNumber("string-test");
    private static final UUID TEST_UUID = UUID.randomUUID();

    @SuppressWarnings("rawtypes")
    @Mock
    private ILeaseManager mockLeaseManager;

    private KinesisClientLibLeaseCoordinator leaseCoordinator;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpLeaseCoordinator() throws ProvisionedThroughputException, DependencyException {
        // Initialize the annotation
        MockitoAnnotations.initMocks(this);
        // Set up lease coordinator
        doReturn(true).when(mockLeaseManager).createLeaseTableIfNotExists(anyLong(), anyLong());
        leaseCoordinator = new KinesisClientLibLeaseCoordinator(mockLeaseManager, WORK_ID, TEST_LONG, TEST_LONG);
    }

    @Test(expected = ShutdownException.class)
    public void testSetCheckpointWithUnownedShardId()
        throws KinesisClientLibException, DependencyException, InvalidStateException, ProvisionedThroughputException {
        final boolean succeess = leaseCoordinator.setCheckpoint(SHARD_ID, TEST_CHKPT, TEST_UUID);
        Assert.assertFalse("Set Checkpoint should return failure", succeess);
        leaseCoordinator.setCheckpoint(SHARD_ID, TEST_CHKPT, TEST_UUID.toString());
    }

    @Test(expected = DependencyException.class)
    public void testWaitLeaseTableTimeout()
        throws DependencyException, ProvisionedThroughputException, IllegalStateException {
        // Set mock lease manager to return false in waiting
        doReturn(false).when(mockLeaseManager).waitUntilLeaseTableExists(anyLong(), anyLong());
        leaseCoordinator.initialize();
    }
}
