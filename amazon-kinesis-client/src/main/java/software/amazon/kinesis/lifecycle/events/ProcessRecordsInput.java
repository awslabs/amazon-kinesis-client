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
package software.amazon.kinesis.lifecycle.events;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Container for the parameters to the ShardRecordProcessor's
 * {@link ShardRecordProcessor#processRecords(ProcessRecordsInput processRecordsInput) processRecords} method.
 */
@Builder(toBuilder = true)
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class ProcessRecordsInput {
    /**
     * The time that this batch of records was received by the KCL.
     */
    private Instant cacheEntryTime;
    /**
     * The time that this batch of records was prepared to be provided to the {@link ShardRecordProcessor}
     */
    private Instant cacheExitTime;
    /**
     * Whether this batch of records is at the end of the shard.
     *
     * {@link ShardRecordProcessor}'s do not need to check this. If this is set the Scheduler will trigger a call to
     * {@link ShardRecordProcessor#shardEnded(ShardEndedInput)} after the completion of the current processing call.
     */
    private boolean isAtShardEnd;
    /**
     * The records received from Kinesis. These records may have been de-aggregated if they were published by the KPL.
     */
    private List<KinesisClientRecord> records;

    /**
     * A checkpointer that the {@link ShardRecordProcessor} can use to checkpoint its progress.
     */
    private RecordProcessorCheckpointer checkpointer;
    /**
     * How far behind this batch of records was when received from Kinesis.
     *
     * This value does not include the {@link #timeSpentInCache()}.
     */
    private Long millisBehindLatest;
    /**
     * A list of child shards if the current GetRecords request reached the shard end.
     * If not at the shard end, this should be an empty list.
     */
    private List<ChildShard> childShards;

    /**
     * How long the records spent waiting to be dispatched to the {@link ShardRecordProcessor}
     *
     * @return the amount of time that records spent waiting before processing.
     */
    public Duration timeSpentInCache() {
        if (cacheEntryTime == null || cacheExitTime == null) {
            return Duration.ZERO;
        }
        return Duration.between(cacheEntryTime, cacheExitTime);
    }
}
