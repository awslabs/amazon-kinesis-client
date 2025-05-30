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

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

@KinesisClientInternalApi
public interface GetRecordsResponseAdapter {

    /**
     * Returns the list of records retrieved from GetRecords.
     * @return list of {@link KinesisClientRecord}
     */
    List<KinesisClientRecord> records();

    /**
     * The number of milliseconds the response is from the tip of the stream.
     * @return long
     */
    Long millisBehindLatest();

    /**
     * Returns the list of child shards of the shard that was retrieved from GetRecords.
     * @return list of {@link ChildShard}
     */
    List<ChildShard> childShards();

    /**
     * Returns the next shard iterator to be used to retrieve next set of records.
     * @return String
     */
    String nextShardIterator();

    /**
     * Returns the request id of the GetRecords operation.
     * @return String containing the request id
     */
    String requestId();
}
