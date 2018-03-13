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

package software.amazon.aws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.BlockingGetRecordsCache;
import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.DataFetchingStrategy;
import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.GetRecordsCache;
import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.GetRecordsRetrievalStrategy;
import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.PrefetchGetRecordsCache;
import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.RecordsFetcherFactory;
import software.amazon.aws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory;
import software.amazon.aws.services.kinesis.metrics.interfaces.IMetricsFactory;

public class RecordsFetcherFactoryTest {
    private String shardId = "TestShard";
    private RecordsFetcherFactory recordsFetcherFactory;

    @Mock
    private GetRecordsRetrievalStrategy getRecordsRetrievalStrategy;
    @Mock
    private IMetricsFactory metricsFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        recordsFetcherFactory = new SimpleRecordsFetcherFactory();
    }

    @Test
    public void createDefaultRecordsFetcherTest() {
        GetRecordsCache recordsCache = recordsFetcherFactory.createRecordsFetcher(getRecordsRetrievalStrategy, shardId,
                metricsFactory, 1);
        assertThat(recordsCache, instanceOf(BlockingGetRecordsCache.class));
    }

    @Test
    public void createPrefetchRecordsFetcherTest() {
        recordsFetcherFactory.setDataFetchingStrategy(DataFetchingStrategy.PREFETCH_CACHED);
        GetRecordsCache recordsCache = recordsFetcherFactory.createRecordsFetcher(getRecordsRetrievalStrategy, shardId,
                metricsFactory, 1);
        assertThat(recordsCache, instanceOf(PrefetchGetRecordsCache.class));
    }

}
