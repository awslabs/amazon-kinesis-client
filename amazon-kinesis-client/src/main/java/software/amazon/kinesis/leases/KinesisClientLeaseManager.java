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

import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * A decoration of ILeaseManager that adds methods to get/update checkpoints.
 */
public interface KinesisClientLeaseManager extends LeaseManager<KinesisClientLease> {

    /**
     * Gets the current checkpoint of the shard. This is useful in the resharding use case
     * where we will wait for the parent shard to complete before starting on the records from a child shard.
     * 
     * @param shardId Checkpoint of this shard will be returned
     * @return Checkpoint of this shard, or null if the shard record doesn't exist.
     * 
     * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
     * @throws InvalidStateException if lease table does not exist
     * @throws DependencyException if DynamoDB update fails in an unexpected way
     */
    ExtendedSequenceNumber getCheckpoint(String shardId) throws ProvisionedThroughputException, InvalidStateException, DependencyException;

}
