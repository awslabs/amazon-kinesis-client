/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.retrieval.fanout.experimental;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.annotations.KinesisClientExperimental;
import software.amazon.kinesis.common.MismatchedRecordReporter;
import software.amazon.kinesis.retrieval.fanout.FanOutRecordsPublisher;

/**
 * A variation of {@link FanOutRecordsPublisher} that provides validation of every record received by the publisher.
 *
 * <h2><strong>This is an experimental class and may be removed at any time</strong></h2>
 */
@Slf4j
@KinesisClientExperimental
public class ExperimentalFanOutRecordsPublisher extends FanOutRecordsPublisher {

    private final MismatchedRecordReporter mismatchedRecordReporter = new MismatchedRecordReporter();

    /**
     * Creates a new FanOutRecordsPublisher.
     *
     * @param kinesis
     *            the kinesis client to use for requests
     * @param shardId
     *            the shardId to retrieve records for
     * @param consumerArn
     */
    public ExperimentalFanOutRecordsPublisher(KinesisAsyncClient kinesis, String shardId, String consumerArn) {
        super(kinesis, shardId, consumerArn);
    }

    @Override
    protected void validateRecords(String shardId, SubscribeToShardEvent event) {
        Map<String, Integer> mismatchedRecords = mismatchedRecordReporter.recordsNotForShard(shardId,
                event.records().stream().map(Record::sequenceNumber));

        if (mismatchedRecords.size() > 0) {
            String mismatchReport = "Received records destined for different shards: " + mismatchedRecords.entrySet().stream()
                    .map(e -> String.format("(%s -> %d)", e.getKey(), e.getValue())).collect(Collectors.joining(", "));
            log.error(mismatchReport);
            throw new IllegalArgumentException(mismatchReport);
        }

        Map<String, Integer> continuationMismatch = mismatchedRecordReporter.recordsNotForShard(shardId, Stream.of(event.continuationSequenceNumber()));
        if (!continuationMismatch.isEmpty()) {
            String continuationReport = "Continuation sequence number not matched to shard: " + mismatchedRecordReporter.makeReport(continuationMismatch);
            log.error(continuationReport);
            throw new IllegalArgumentException(continuationReport);
        }

    }
}
