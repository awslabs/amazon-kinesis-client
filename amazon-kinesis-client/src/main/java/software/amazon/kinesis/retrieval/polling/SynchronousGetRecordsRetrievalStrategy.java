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
