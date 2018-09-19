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
package software.amazon.kinesis.common;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import software.amazon.kinesis.annotations.KinesisClientExperimental;
import software.amazon.kinesis.checkpoint.SequenceNumberValidator;

@KinesisClientExperimental
public class MismatchedRecordReporter {

    private final SequenceNumberValidator sequenceNumberValidator = new SequenceNumberValidator();

    public Map<String, Integer> recordsNotForShard(String shardId, Stream<String> sequenceNumbers) {
        return sequenceNumbers.map(seq -> {
            Optional<String> res = sequenceNumberValidator.shardIdFor(seq);
            if (!res.isPresent()) {
                throw new IllegalArgumentException("Unable to validate sequence number of " + seq);
            }
            return res.get();
        }).filter(s -> !StringUtils.equalsIgnoreCase(s, shardId)).collect(Collectors.groupingBy(Function.identity()))
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
    }

    public String makeReport(Map<String, Integer> mismatchedRecords) {
        return mismatchedRecords.entrySet().stream().map(e -> String.format("(%s -> %d)", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));
    }
}
