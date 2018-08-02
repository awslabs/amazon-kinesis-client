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
package software.amazon.kinesis.retrieval.polling;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.DataFetchingStrategy;
import software.amazon.kinesis.retrieval.GetRecordsRetrievalStrategy;
import software.amazon.kinesis.retrieval.RecordsFetcherFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;

public class RecordsFetcherFactoryTest {
    private String shardId = "TestShard";
    private RecordsFetcherFactory recordsFetcherFactory;

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    @Mock
    private MetricsFactory metricsFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        recordsFetcherFactory = new SimpleRecordsFetcherFactory();
    }

    @Test
    @Ignore
//    TODO: remove test no longer holds true
    public void createDefaultRecordsFetcherTest() {
        RecordsPublisher recordsCache = recordsFetcherFactory.createRecordsFetcher(getRecordsRetrievalStrategy, shardId,
                metricsFactory, 1);
        assertThat(recordsCache, instanceOf(BlockingRecordsPublisher.class));
    }

    @Test
    public void createPrefetchRecordsFetcherTest() {
        recordsFetcherFactory.dataFetchingStrategy(DataFetchingStrategy.PREFETCH_CACHED);
        RecordsPublisher recordsCache = recordsFetcherFactory.createRecordsFetcher(getRecordsRetrievalStrategy, shardId,
                metricsFactory, 1);
        assertThat(recordsCache, instanceOf(PrefetchRecordsPublisher.class));
    }

}
