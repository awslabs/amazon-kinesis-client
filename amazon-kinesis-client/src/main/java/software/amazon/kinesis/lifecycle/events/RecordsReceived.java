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
package software.amazon.kinesis.lifecycle.events;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.amazonaws.services.kinesis.model.Record;

import lombok.Data;
import software.amazon.kinesis.lifecycle.ProcessRecordsInput;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;

@Data
public class RecordsReceived {

    private final Instant cacheEntryTime;
    private final Instant cacheExitTime;
    private final boolean isAtShardEnd;
    private final List<Record> records;
    private final IRecordProcessorCheckpointer checkpointer;
    private Duration timeBehindLatest;

    public ProcessRecordsInput toProcessRecordsInput() {
        return new ProcessRecordsInput(cacheEntryTime, cacheExitTime, isAtShardEnd, records, checkpointer,
                timeBehindLatest.toMillis());
    }

}
