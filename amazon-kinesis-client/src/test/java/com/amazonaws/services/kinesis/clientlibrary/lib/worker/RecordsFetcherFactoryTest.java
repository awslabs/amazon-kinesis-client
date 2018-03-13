package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

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
