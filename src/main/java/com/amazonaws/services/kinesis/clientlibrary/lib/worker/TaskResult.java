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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.model.ChildShard;

import java.util.List;

/**
 * Used to capture information from a task that we want to communicate back to the higher layer.
 * E.g. exception thrown when executing the task, if we reach end of a shard.
 */
public class TaskResult {

    // Did we reach the end of the shard while processing this task.
    private boolean shardEndReached;

    // Any exception caught while executing the task.
    private Exception exception;

    // List of childShards of the current shard. This field is only required for the task result when we reach end of a shard.
    private List<ChildShard> childShards;

    private boolean leaseNotFound;

    /**
     * @return the shardEndReached
     */
    public boolean isShardEndReached() {
        return shardEndReached;
    }

    /**
     * @return the list of childShards.
     */
    protected List<ChildShard> getChildShards() { return childShards; }

    /**
     * @param shardEndReached the shardEndReached to set
     */
    protected void setShardEndReached(boolean shardEndReached) {
        this.shardEndReached = shardEndReached;
    }

    /**
     * @param childShards the list of childShards to set
     */
    protected void setChildShards(List<ChildShard> childShards) { this.childShards = childShards; }

    public boolean isLeaseNotFound() {
        return leaseNotFound;
    }

    public void leaseNotFound() {
        this.leaseNotFound = true;
    }

    /**
     * @return the exception
     */
    public Exception getException() {
        return exception;
    }

    /**
     * @param e Any exception encountered when running the process task.
     */
    public TaskResult(Exception e) {
        this(e, false);
    }

    /**
     * @param isShardEndReached Whether we reached the end of the shard (no more records will ever be fetched)
     */
    TaskResult(boolean isShardEndReached) {
        this(null, isShardEndReached);
    }

    /**
     * @param e Any exception encountered when executing task.
     * @param isShardEndReached Whether we reached the end of the shard (no more records will ever be fetched)
     */
    TaskResult(Exception e, boolean isShardEndReached) {
        this.exception = e;
        this.shardEndReached = isShardEndReached;
    }

    TaskResult(Exception e, boolean isShardEndReached, List<ChildShard> childShards) {
        this.exception = e;
        this.shardEndReached = isShardEndReached;
        this.childShards = childShards;
    }

}
