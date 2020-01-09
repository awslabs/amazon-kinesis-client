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
package com.amazonaws.services.kinesis.leases.impl;

import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.IKinesisClientLeaseManager;

/**
 * An implementation of LeaseManager for the KinesisClientLibrary - takeLease updates the ownerSwitchesSinceCheckpoint field.
 */
public class KinesisClientLeaseManager extends LeaseManager<KinesisClientLease> implements IKinesisClientLeaseManager {

    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(KinesisClientLeaseManager.class);

    /**
     * Constructor.
     *
     * @param table Leases table
     * @param dynamoDBClient DynamoDB client to use
     */
    @Deprecated
    public KinesisClientLeaseManager(String table, AmazonDynamoDB dynamoDBClient) {
        this(table, dynamoDBClient, false, KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE);
    }

    /**
     * Constructor.
     * 
     * @param table Leases table
     * @param dynamoDBClient DynamoDB client to use
     */
    public KinesisClientLeaseManager(String table, AmazonDynamoDB dynamoDBClient, BillingMode billingMode) {
        this(table, dynamoDBClient, false, billingMode);
    }

    /**
     * Constructor for integration tests - see comment on superclass for documentation on setting the consistentReads
     * flag.
     *
     * @param table leases table
     * @param dynamoDBClient DynamoDB client to use
     * @param consistentReads true if we want consistent reads for testing purposes.
     */
    @Deprecated
    public KinesisClientLeaseManager(String table, AmazonDynamoDB dynamoDBClient, boolean consistentReads) {
        super(table, dynamoDBClient, new KinesisClientLeaseSerializer(), consistentReads, KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE);
    }

    /**
     * Constructor for integration tests - see comment on superclass for documentation on setting the consistentReads
     * flag.
     * 
     * @param table leases table
     * @param dynamoDBClient DynamoDB client to use
     * @param consistentReads true if we want consistent reads for testing purposes.
     */
    public KinesisClientLeaseManager(String table, AmazonDynamoDB dynamoDBClient, boolean consistentReads, BillingMode billingMode) {
        super(table, dynamoDBClient, new KinesisClientLeaseSerializer(), consistentReads, billingMode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean takeLease(KinesisClientLease lease, String newOwner)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        String oldOwner = lease.getLeaseOwner();

        boolean result = super.takeLease(lease, newOwner);

        if (oldOwner != null && !oldOwner.equals(newOwner)) {
            lease.setOwnerSwitchesSinceCheckpoint(lease.getOwnerSwitchesSinceCheckpoint() + 1);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExtendedSequenceNumber getCheckpoint(String shardId)
        throws ProvisionedThroughputException, InvalidStateException, DependencyException {
    	ExtendedSequenceNumber checkpoint = null;
        KinesisClientLease lease = getLease(shardId);
        if (lease != null) {
            checkpoint = lease.getCheckpoint();
        }
        return checkpoint;
    }
}
