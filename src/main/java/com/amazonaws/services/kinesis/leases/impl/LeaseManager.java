/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.leases.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.leases.util.DynamoUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseSerializer;

/**
 * An implementation of ILeaseManager that uses DynamoDB.
 */
public class LeaseManager<T extends Lease> implements ILeaseManager<T> {

    private static final Log LOG = LogFactory.getLog(LeaseManager.class);

    protected String table;
    protected AmazonDynamoDB dynamoDBClient;
    protected ILeaseSerializer<T> serializer;
    protected boolean consistentReads;

    /**
     * Constructor.
     * 
     * @param table leases table
     * @param dynamoDBClient DynamoDB client to use
     * @param serializer LeaseSerializer to use to convert to/from DynamoDB objects.
     */
    public LeaseManager(String table, AmazonDynamoDB dynamoDBClient, ILeaseSerializer<T> serializer) {
        this(table, dynamoDBClient, serializer, false);
    }

    /**
     * Constructor for test cases - allows control of consistent reads. Consistent reads should only be used for testing
     * - our code is meant to be resilient to inconsistent reads. Using consistent reads during testing speeds up
     * execution of simple tests (you don't have to wait out the consistency window). Test cases that want to experience
     * eventual consistency should not set consistentReads=true.
     * 
     * @param table leases table
     * @param dynamoDBClient DynamoDB client to use
     * @param serializer lease serializer to use
     * @param consistentReads true if we want consistent reads for testing purposes.
     */
    public LeaseManager(String table, AmazonDynamoDB dynamoDBClient, ILeaseSerializer<T> serializer, boolean consistentReads) {
        verifyNotNull(table, "Table name cannot be null");
        verifyNotNull(dynamoDBClient, "dynamoDBClient cannot be null");
        verifyNotNull(serializer, "ILeaseSerializer cannot be null");

        this.table = table;
        this.dynamoDBClient = dynamoDBClient;
        this.consistentReads = consistentReads;
        this.serializer = serializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createLeaseTableIfNotExists(Long readCapacity, Long writeCapacity)
        throws ProvisionedThroughputException, DependencyException {
        verifyNotNull(readCapacity, "readCapacity cannot be null");
        verifyNotNull(writeCapacity, "writeCapacity cannot be null");

        try {
            if (tableStatus() != null) {
                return false;
            }
        } catch (DependencyException de) {
            //
            // Something went wrong with DynamoDB
            //
            LOG.error("Failed to get table status for " + table, de);
        }
        CreateTableRequest request = new CreateTableRequest();
        request.setTableName(table);
        request.setKeySchema(serializer.getKeySchema());
        request.setAttributeDefinitions(serializer.getAttributeDefinitions());

        ProvisionedThroughput throughput = new ProvisionedThroughput();
        throughput.setReadCapacityUnits(readCapacity);
        throughput.setWriteCapacityUnits(writeCapacity);
        request.setProvisionedThroughput(throughput);

        try {
            dynamoDBClient.createTable(request);
        } catch (ResourceInUseException e) {
            LOG.info("Table " + table + " already exists.");
            return false;
        } catch (LimitExceededException e) {
            throw new ProvisionedThroughputException("Capacity exceeded when creating table " + table, e);
        } catch (AmazonClientException e) {
            throw new DependencyException(e);
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean leaseTableExists() throws DependencyException {
        return TableStatus.ACTIVE == tableStatus();
    }

    private TableStatus tableStatus() throws DependencyException {
        DescribeTableRequest request = new DescribeTableRequest();

        request.setTableName(table);

        DescribeTableResult result;
        try {
            result = dynamoDBClient.describeTable(request);
        } catch (ResourceNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Got ResourceNotFoundException for table %s in leaseTableExists, returning false.",
                        table));
            }
            return null;
        } catch (AmazonClientException e) {
            throw new DependencyException(e);
        }

        TableStatus tableStatus = TableStatus.fromValue(result.getTable().getTableStatus());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Lease table exists and is in status " + tableStatus);
        }

        return tableStatus;
    }

    @Override
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException {
        long sleepTimeRemaining = TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (!leaseTableExists()) {
            if (sleepTimeRemaining <= 0) {
                return false;
            }

            long timeToSleepMillis = Math.min(TimeUnit.SECONDS.toMillis(secondsBetweenPolls), sleepTimeRemaining);

            sleepTimeRemaining -= sleep(timeToSleepMillis);
        }

        return true;
    }

    /**
     * Exposed for testing purposes.
     * 
     * @param timeToSleepMillis time to sleep in milliseconds
     * 
     * @return actual time slept in millis
     */
    long sleep(long timeToSleepMillis) {
        long startTime = System.currentTimeMillis();

        try {
            Thread.sleep(timeToSleepMillis);
        } catch (InterruptedException e) {
            LOG.debug("Interrupted while sleeping");
        }

        return System.currentTimeMillis() - startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaseTableEmpty() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(1).isEmpty();
    }

    /**
     * List with the given page size. Package access for integration testing.
     * 
     * @param limit number of items to consider at a time - used by integration tests to force paging.
     * @return list of leases
     * @throws InvalidStateException if table does not exist
     * @throws DependencyException if DynamoDB scan fail in an unexpected way
     * @throws ProvisionedThroughputException if DynamoDB scan fail due to exceeded capacity
     */
    List<T> list(Integer limit) throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Listing leases from table " + table);
        }

        ScanRequest scanRequest = new ScanRequest();
        scanRequest.setTableName(table);
        if (limit != null) {
            scanRequest.setLimit(limit);
        }

        try {
            ScanResult scanResult = dynamoDBClient.scan(scanRequest);
            List<T> result = new ArrayList<T>();

            while (scanResult != null) {
                for (Map<String, AttributeValue> item : scanResult.getItems()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Got item " + item.toString() + " from DynamoDB.");
                    }

                    result.add(serializer.fromDynamoRecord(item));
                }

                Map<String, AttributeValue> lastEvaluatedKey = scanResult.getLastEvaluatedKey();
                if (lastEvaluatedKey == null) {
                    // Signify that we're done.
                    scanResult = null;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("lastEvaluatedKey was null - scan finished.");
                    }
                } else {
                    // Make another request, picking up where we left off.
                    scanRequest.setExclusiveStartKey(lastEvaluatedKey);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("lastEvaluatedKey was " + lastEvaluatedKey + ", continuing scan.");
                    }

                    scanResult = dynamoDBClient.scan(scanRequest);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Listed " + result.size() + " leases from table " + table);
            }

            return result;
        } catch (ResourceNotFoundException e) {
            throw new InvalidStateException("Cannot scan lease table " + table + " because it does not exist.", e);
        } catch (ProvisionedThroughputExceededException e) {
            throw new ProvisionedThroughputException(e);
        } catch (AmazonClientException e) {
            throw new DependencyException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createLeaseIfNotExists(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating lease " + lease);
        }

        PutItemRequest request = new PutItemRequest();
        request.setTableName(table);
        request.setItem(serializer.toDynamoRecord(lease));
        request.setExpected(serializer.getDynamoNonexistantExpectation());

        try {
            dynamoDBClient.putItem(request);
        } catch (ConditionalCheckFailedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Did not create lease " + lease + " because it already existed");
            }

            return false;
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("create", lease.getLeaseKey(), e);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getLease(String leaseKey)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(leaseKey, "leaseKey cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting lease with key " + leaseKey);
        }

        GetItemRequest request = new GetItemRequest();
        request.setTableName(table);
        request.setKey(serializer.getDynamoHashKey(leaseKey));
        request.setConsistentRead(consistentReads);

        try {
            GetItemResult result = dynamoDBClient.getItem(request);

            Map<String, AttributeValue> dynamoRecord = result.getItem();
            if (dynamoRecord == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No lease found with key " + leaseKey + ", returning null.");
                }

                return null;
            } else {
                T lease = serializer.fromDynamoRecord(dynamoRecord);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got lease " + lease);
                }

                return lease;
            }
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("get", leaseKey, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean renewLease(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Renewing lease with key " + lease.getLeaseKey());
        }

        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(table);
        request.setKey(serializer.getDynamoHashKey(lease));
        request.setExpected(serializer.getDynamoLeaseCounterExpectation(lease));
        request.setAttributeUpdates(serializer.getDynamoLeaseCounterUpdate(lease));

        try {
            dynamoDBClient.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewal failed for lease with key " + lease.getLeaseKey()
                        + " because the lease counter was not " + lease.getLeaseCounter());
            }

            // If we had a spurious retry during the Dynamo update, then this conditional PUT failure
            // might be incorrect. So, we get the item straight away and check if the lease owner + lease counter
            // are what we expected.
            String expectedOwner = lease.getLeaseOwner();
            Long expectedCounter = lease.getLeaseCounter() + 1;
            T updatedLease = getLease(lease.getLeaseKey());
            if (updatedLease == null || !expectedOwner.equals(updatedLease.getLeaseOwner()) ||
                    !expectedCounter.equals(updatedLease.getLeaseCounter())) {
                return false;
            }

            LOG.info("Detected spurious renewal failure for lease with key " + lease.getLeaseKey()
                    + ", but recovered");
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("renew", lease.getLeaseKey(), e);
        }

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean takeLease(T lease, String owner)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");
        verifyNotNull(owner, "owner cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Taking lease with leaseKey %s from %s to %s",
                    lease.getLeaseKey(),
                    lease.getLeaseOwner() == null ? "nobody" : lease.getLeaseOwner(),
                    owner));
        }

        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(table);
        request.setKey(serializer.getDynamoHashKey(lease));
        request.setExpected(serializer.getDynamoLeaseCounterExpectation(lease));

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoTakeLeaseUpdate(lease, owner));
        request.setAttributeUpdates(updates);

        try {
            dynamoDBClient.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewal failed for lease with key " + lease.getLeaseKey()
                        + " because the lease counter was not " + lease.getLeaseCounter());
            }

            return false;
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("take", lease.getLeaseKey(), e);
        }

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        lease.setLeaseOwner(owner);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean evictLease(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Evicting lease with leaseKey %s owned by %s",
                    lease.getLeaseKey(),
                    lease.getLeaseOwner()));
        }

        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(table);
        request.setKey(serializer.getDynamoHashKey(lease));
        request.setExpected(serializer.getDynamoLeaseOwnerExpectation(lease));

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoEvictLeaseUpdate(lease));
        request.setAttributeUpdates(updates);

        try {
            dynamoDBClient.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lease eviction failed for lease with key " + lease.getLeaseKey()
                        + " because the lease owner was not " + lease.getLeaseOwner());
            }

            return false;
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("evict", lease.getLeaseKey(), e);
        }

        lease.setLeaseOwner(null);
        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public void deleteAll() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        List<T> allLeases = listLeases();

        LOG.warn("Deleting " + allLeases.size() + " items from table " + table);

        for (T lease : allLeases) {
            DeleteItemRequest deleteRequest = new DeleteItemRequest();
            deleteRequest.setTableName(table);
            deleteRequest.setKey(serializer.getDynamoHashKey(lease));

            dynamoDBClient.deleteItem(deleteRequest);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteLease(T lease) throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Deleting lease with leaseKey %s", lease.getLeaseKey()));
        }

        DeleteItemRequest deleteRequest = new DeleteItemRequest();
        deleteRequest.setTableName(table);
        deleteRequest.setKey(serializer.getDynamoHashKey(lease));

        try {
            dynamoDBClient.deleteItem(deleteRequest);
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("delete", lease.getLeaseKey(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateLease(T lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        verifyNotNull(lease, "lease cannot be null");

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Updating lease %s", lease));
        }

        UpdateItemRequest request = new UpdateItemRequest();
        request.setTableName(table);
        request.setKey(serializer.getDynamoHashKey(lease));
        request.setExpected(serializer.getDynamoLeaseCounterExpectation(lease));

        Map<String, AttributeValueUpdate> updates = serializer.getDynamoLeaseCounterUpdate(lease);
        updates.putAll(serializer.getDynamoUpdateLeaseUpdate(lease));
        request.setAttributeUpdates(updates);

        try {
            dynamoDBClient.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lease update failed for lease with key " + lease.getLeaseKey()
                        + " because the lease counter was not " + lease.getLeaseCounter());
            }

            return false;
        } catch (AmazonClientException e) {
            throw convertAndRethrowExceptions("update", lease.getLeaseKey(), e);
        }

        lease.setLeaseCounter(lease.getLeaseCounter() + 1);
        return true;
    }

    /*
     * This method contains boilerplate exception handling - it throws or returns something to be thrown. The
     * inconsistency there exists to satisfy the compiler when this method is used at the end of non-void methods.
     */
    protected DependencyException convertAndRethrowExceptions(String operation, String leaseKey, AmazonClientException e)
        throws ProvisionedThroughputException, InvalidStateException {
        if (e instanceof ProvisionedThroughputExceededException) {
            LOG.warn("Provisioned Throughput on the lease table has been exceeded. It's recommended that you increase the IOPs on the table. Failure to increase the IOPs may cause the application to not make progress.");
            throw new ProvisionedThroughputException(e);
        } else if (e instanceof ResourceNotFoundException) {
            // @formatter:on
            throw new InvalidStateException(String.format("Cannot %s lease with key %s because table %s does not exist.",
                    operation,
                    leaseKey,
                    table),
                    e);
            //@formatter:off
        } else {
            return new DependencyException(e);
        }
    }
    
    private void verifyNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

}
