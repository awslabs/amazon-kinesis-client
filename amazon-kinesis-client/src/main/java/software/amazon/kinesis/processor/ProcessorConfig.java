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

package software.amazon.kinesis.processor;

        import lombok.Data;
        import lombok.NonNull;
        import lombok.experimental.Accessors;

/**
 * Used by the KCL to configure the processor for processing the records.
 */
@Data
@Accessors(fluent = true)
public class ProcessorConfig {
    /**
     *
     */
    @NonNull
    private final ShardRecordProcessorFactory shardRecordProcessorFactory;

    /**
     * Don't call processRecords() on the record processor for empty record lists.
     *
     * <p>Default value: false</p>
     */
    private boolean callProcessRecordsEvenForEmptyRecordList = false;

}
