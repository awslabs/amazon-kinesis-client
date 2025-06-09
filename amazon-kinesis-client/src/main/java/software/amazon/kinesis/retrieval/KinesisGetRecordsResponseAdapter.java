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

package software.amazon.kinesis.retrieval;

import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

@Getter
@RequiredArgsConstructor
@KinesisClientInternalApi
@Accessors(fluent = true)
@EqualsAndHashCode
public class KinesisGetRecordsResponseAdapter implements GetRecordsResponseAdapter {

    private final GetRecordsResponse getRecordsResponse;

    @Override
    public List<KinesisClientRecord> records() {
        return getRecordsResponse.records().stream()
                .map(KinesisClientRecord::fromRecord)
                .collect(Collectors.toList());
    }

    @Override
    public Long millisBehindLatest() {
        return getRecordsResponse.millisBehindLatest();
    }

    @Override
    public List<ChildShard> childShards() {
        return getRecordsResponse.childShards();
    }

    @Override
    public String nextShardIterator() {
        return getRecordsResponse.nextShardIterator();
    }

    @Override
    public String requestId() {
        return getRecordsResponse.responseMetadata().requestId();
    }
}
