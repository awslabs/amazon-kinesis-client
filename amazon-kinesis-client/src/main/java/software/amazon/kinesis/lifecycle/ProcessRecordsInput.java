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
package software.amazon.kinesis.lifecycle;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import lombok.AllArgsConstructor;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.Record;

import lombok.Getter;
import software.amazon.kinesis.processor.RecordProcessor;

/**
 * Container for the parameters to the IRecordProcessor's
 * {@link RecordProcessor#processRecords(
 * ProcessRecordsInput processRecordsInput) processRecords} method.
 */
@AllArgsConstructor
public class ProcessRecordsInput {
    @Getter
    private Instant cacheEntryTime;
    @Getter
    private Instant cacheExitTime;
    @Getter
    private boolean isAtShardEnd;
    private List<Record> records;
    private IRecordProcessorCheckpointer checkpointer;
    private Long millisBehindLatest;

    /**
     * Default constructor.
     */
    public ProcessRecordsInput() {
    }

    /**
     * Get records.
     *
     * @return Data records to be processed
     */
    public List<Record> getRecords() {
        return records;
    }

    /**
     * Set records.
     *
     * @param records Data records to be processed
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public ProcessRecordsInput withRecords(List<Record> records) {
        this.records = records;
        return this;
    }

    /**
     * Get Checkpointer.
     *
     * @return RecordProcessor should use this instance to checkpoint their progress.
     */
    public IRecordProcessorCheckpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * Set Checkpointer.
     *
     * @param checkpointer RecordProcessor should use this instance to checkpoint their progress.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public ProcessRecordsInput withCheckpointer(IRecordProcessorCheckpointer checkpointer) {
        this.checkpointer = checkpointer;
        return this;
    }

    /**
     * Get milliseconds behind latest.
     *
     * @return The number of milliseconds this batch of records is from the tip of the stream,
     *         indicating how far behind current time the record processor is.
     */
    public Long getMillisBehindLatest() {
        return millisBehindLatest;
    }

    /**
     * Set milliseconds behind latest.
     *
     * @param millisBehindLatest The number of milliseconds this batch of records is from the tip of the stream,
     *         indicating how far behind current time the record processor is.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    public ProcessRecordsInput withMillisBehindLatest(Long millisBehindLatest) {
        this.millisBehindLatest = millisBehindLatest;
        return this;
    }
    
    public ProcessRecordsInput withCacheEntryTime(Instant cacheEntryTime) {
        this.cacheEntryTime = cacheEntryTime;
        return this;
    }
    
    public ProcessRecordsInput withCacheExitTime(Instant cacheExitTime) {
        this.cacheExitTime = cacheExitTime;
        return this;
    }
    
    public Duration getTimeSpentInCache() {
        if (cacheEntryTime == null || cacheExitTime == null) {
            return Duration.ZERO;
        }
        return Duration.between(cacheEntryTime, cacheExitTime);
    }
}
