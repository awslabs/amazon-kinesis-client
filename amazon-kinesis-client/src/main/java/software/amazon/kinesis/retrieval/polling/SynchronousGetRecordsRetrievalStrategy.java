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
package software.amazon.kinesis.retrieval.polling;

import lombok.Data;
import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;

/**
 *
 */
@Data
@KinesisClientInternalApi
public class SynchronousGetRecordsRetrievalStrategy implements GetRecordsRetrievalStrategy {
    @NonNull
    private final KinesisDataFetcher dataFetcher;

    @Override
    public GetRecordsResponse getRecords(final int maxRecords) {
        return dataFetcher.getRecords().accept();
    }

    @Override
    public void shutdown() {
        //
        // Does nothing as this retriever doesn't manage any resources
        //
    }

    @Override
    public boolean isShutdown() {
        return false;
    }
    
    @Override
    public KinesisDataFetcher getDataFetcher() {
        return dataFetcher;
    }
}
