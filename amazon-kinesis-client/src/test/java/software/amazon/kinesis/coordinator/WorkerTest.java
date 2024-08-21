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
package software.amazon.kinesis.coordinator;

/**
 * Unit tests of Worker.
 */
// TODO: remove this test class
public class WorkerTest {
    /*// @Rule
    // public Timeout timeout = new Timeout((int)TimeUnit.SECONDS.toMillis(30));

    private final NullMetricsFactory nullMetricsFactory = new NullMetricsFactory();
    private final long taskBackoffTimeMillis = 1L;
    private final long failoverTimeMillis = 5L;
    private final boolean callProcessRecordsForEmptyRecordList = false;
    private final long parentShardPollIntervalMillis = 5L;
    private final long shardSyncIntervalMillis = 5L;
    private final boolean cleanupLeasesUponShardCompletion = true;
    // We don't want any of these tests to run checkpoint validation
    private final boolean skipCheckpointValidationValue = false;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private final ShardPrioritization shardPrioritization = new NoOpShardPrioritization();

    private static final String KINESIS_SHARD_ID_FORMAT = "kinesis-0-0-%d";
    private static final String CONCURRENCY_TOKEN_FORMAT = "testToken-%d";

    private RecordsFetcherFactory recordsFetcherFactory;
    private KinesisClientLibConfiguration config;

    @Mock
    private KinesisClientLibLeaseCoordinator leaseCoordinator;
    @Mock
    private ILeaseManager<KinesisClientLease> leaseRefresher;
    @Mock
    private software.amazon.kinesis.processor.IRecordProcessorFactory v1RecordProcessorFactory;
    @Mock
    private IKinesisProxy proxy;
    @Mock
    private WorkerThreadPoolExecutor executorService;
    @Mock
    private WorkerCWMetricsFactory cwMetricsFactory;
    @Mock
    private IKinesisProxy kinesisProxy;
    @Mock
    private IRecordProcessorFactory v2RecordProcessorFactory;
    @Mock
    private IRecordProcessor v2RecordProcessor;
    @Mock
    private ShardConsumer shardConsumer;
    @Mock
    private Future<TaskResult> taskFuture;
    @Mock
    private TaskResult taskResult;
    @Mock
    private WorkerStateChangeListener workerStateChangeListener;

    @Before
    public void setup() {
        config = spy(new KinesisClientLibConfiguration("app", null, null, null));
        recordsFetcherFactory = spy(new SimpleRecordsFetcherFactory());
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
    }

    // CHECKSTYLE:IGNORE AnonInnerLengthCheck FOR NEXT 50 LINES
    private static final software.amazon.kinesis.processor.IRecordProcessorFactory SAMPLE_RECORD_PROCESSOR_FACTORY =
            new software.amazon.kinesis.processor.IRecordProcessorFactory() {

        @Override
        public software.amazon.kinesis.processor.IRecordProcessor createProcessor() {
            return new IRecordProcessor() {

                @Override
                public void initialize(final InitializationInput initializationInput) {

                }

                @Override
                public void processRecords(final ProcessRecordsInput processRecordsInput) {
                    try {
                        processRecordsInput.checkpointer().checkpoint();
                    } catch (KinesisClientLibNonRetryableException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void shutdown(final ShutdownInput shutdownInput) {
                    if (shutdownInput.shutdownReason() == ShutdownReason.TERMINATE) {
                        try {
                            shutdownInput.checkpointer().checkpoint();
                        } catch (KinesisClientLibNonRetryableException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

            };
        }
    };

    private static final IRecordProcessorFactory SAMPLE_RECORD_PROCESSOR_FACTORY_V2 = SAMPLE_RECORD_PROCESSOR_FACTORY;

    */
    /*
     * Test method for {@link Worker#getApplicationName()}.
     */
    /*
    @Test
    public final void testGetStageName() {
        final String stageName = "testStageName";
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        Worker worker = new Worker(v1RecordProcessorFactory, config);
        Assert.assertEquals(stageName, worker.getApplicationName());
    }

    @Test
    public final void testCreateOrGetShardConsumer() {
        final String stageName = "testStageName";
        IRecordProcessorFactory streamletFactory = SAMPLE_RECORD_PROCESSOR_FACTORY_V2;
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        IKinesisProxy proxy = null;
        ICheckpoint checkpoint = null;
        int maxRecords = 1;
        int idleTimeInMilliseconds = 1000;
        StreamConfig streamConfig =
                new StreamConfig(proxy,
                        maxRecords,
                        idleTimeInMilliseconds,
                        callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);
        final String testConcurrencyToken = "testToken";
        final String anotherConcurrencyToken = "anotherTestToken";
        final String dummyKinesisShardId = "kinesis-0-0";
        ExecutorService execService = null;

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        Worker worker =
                new Worker(stageName,
                        streamletFactory,
                        config,
                        streamConfig, INITIAL_POSITION_LATEST,
                        parentShardPollIntervalMillis,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        checkpoint,
                        leaseCoordinator,
                        execService,
                        nullMetricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);
        ShardInfo shardInfo = new ShardInfo(dummyKinesisShardId, testConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardConsumer consumer = worker.createOrGetShardConsumer(shardInfo, streamletFactory);
        Assert.assertNotNull(consumer);
        ShardConsumer consumer2 = worker.createOrGetShardConsumer(shardInfo, streamletFactory);
        Assert.assertSame(consumer, consumer2);
        ShardInfo shardInfoWithSameShardIdButDifferentConcurrencyToken =
                new ShardInfo(dummyKinesisShardId, anotherConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardConsumer consumer3 =
                worker.createOrGetShardConsumer(shardInfoWithSameShardIdButDifferentConcurrencyToken, streamletFactory);
        Assert.assertNotNull(consumer3);
        Assert.assertNotSame(consumer3, consumer);
    }

    @Test
    public void testWorkerLoopWithCheckpoint() {
        final String stageName = "testStageName";
        IRecordProcessorFactory streamletFactory = SAMPLE_RECORD_PROCESSOR_FACTORY_V2;
        IKinesisProxy proxy = null;
        ICheckpoint checkpoint = null;
        int maxRecords = 1;
        int idleTimeInMilliseconds = 1000;
        StreamConfig streamConfig = new StreamConfig(proxy, maxRecords, idleTimeInMilliseconds,
                callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);

        ExecutorService execService = null;

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        List<ShardInfo> initialState = createShardInfoList(ExtendedSequenceNumber.TRIM_HORIZON);
        List<ShardInfo> firstCheckpoint = createShardInfoList(new ExtendedSequenceNumber("1000"));
        List<ShardInfo> secondCheckpoint = createShardInfoList(new ExtendedSequenceNumber("2000"));

        when(leaseCoordinator.getCurrentAssignments()).thenReturn(initialState).thenReturn(firstCheckpoint)
                .thenReturn(secondCheckpoint);

        Worker worker = new Worker(stageName,
                streamletFactory,
                config,
                streamConfig,
                INITIAL_POSITION_LATEST,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                checkpoint,
                leaseCoordinator,
                execService,
                nullMetricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                shardPrioritization);

        Worker workerSpy = spy(worker);

        doReturn(shardConsumer).when(workerSpy).buildConsumer(eq(initialState.get(0)));
        workerSpy.runProcessLoop();
        workerSpy.runProcessLoop();
        workerSpy.runProcessLoop();

        verify(workerSpy).buildConsumer(same(initialState.get(0)));
        verify(workerSpy, never()).buildConsumer(same(firstCheckpoint.get(0)));
        verify(workerSpy, never()).buildConsumer(same(secondCheckpoint.get(0)));

    }

    private List<ShardInfo> createShardInfoList(ExtendedSequenceNumber... sequenceNumbers) {
        List<ShardInfo> result = new ArrayList<>(sequenceNumbers.length);
        assertThat(sequenceNumbers.length, greaterThanOrEqualTo(1));
        for (int i = 0; i < sequenceNumbers.length; ++i) {
            result.add(new ShardInfo(adjustedShardId(i), adjustedConcurrencyToken(i), null, sequenceNumbers[i]));
        }
        return result;
    }

    private String adjustedShardId(int index) {
        return String.format(KINESIS_SHARD_ID_FORMAT, index);
    }

    private String adjustedConcurrencyToken(int index) {
        return String.format(CONCURRENCY_TOKEN_FORMAT, index);
    }

    @Test
    public final void testCleanupShardConsumers() {
        final String stageName = "testStageName";
        IRecordProcessorFactory streamletFactory = SAMPLE_RECORD_PROCESSOR_FACTORY_V2;
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        IKinesisProxy proxy = null;
        ICheckpoint checkpoint = null;
        int maxRecords = 1;
        int idleTimeInMilliseconds = 1000;
        StreamConfig streamConfig =
                new StreamConfig(proxy,
                        maxRecords,
                        idleTimeInMilliseconds,
                        callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);
        final String concurrencyToken = "testToken";
        final String anotherConcurrencyToken = "anotherTestToken";
        final String dummyKinesisShardId = "kinesis-0-0";
        final String anotherDummyKinesisShardId = "kinesis-0-1";
        ExecutorService execService = null;
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        Worker worker =
                new Worker(stageName,
                        streamletFactory,
                        config,
                        streamConfig, INITIAL_POSITION_LATEST,
                        parentShardPollIntervalMillis,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        checkpoint,
                        leaseCoordinator,
                        execService,
                        nullMetricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);

        ShardInfo shardInfo1 = new ShardInfo(dummyKinesisShardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardInfo duplicateOfShardInfo1ButWithAnotherConcurrencyToken =
                new ShardInfo(dummyKinesisShardId, anotherConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ShardInfo shardInfo2 = new ShardInfo(anotherDummyKinesisShardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        ShardConsumer consumerOfShardInfo1 = worker.createOrGetShardConsumer(shardInfo1, streamletFactory);
        ShardConsumer consumerOfDuplicateOfShardInfo1ButWithAnotherConcurrencyToken =
                worker.createOrGetShardConsumer(duplicateOfShardInfo1ButWithAnotherConcurrencyToken, streamletFactory);
        ShardConsumer consumerOfShardInfo2 = worker.createOrGetShardConsumer(shardInfo2, streamletFactory);

        Set<ShardInfo> assignedShards = new HashSet<ShardInfo>();
        assignedShards.add(shardInfo1);
        assignedShards.add(shardInfo2);
        worker.cleanupShardConsumers(assignedShards);

        // verify shard consumer not present in assignedShards is shut down
        Assert.assertTrue(consumerOfDuplicateOfShardInfo1ButWithAnotherConcurrencyToken.isShutdownRequested());
        // verify shard consumers present in assignedShards aren't shut down
        Assert.assertFalse(consumerOfShardInfo1.isShutdownRequested());
        Assert.assertFalse(consumerOfShardInfo2.isShutdownRequested());
    }

    @Test
    public final void testInitializationFailureWithRetries() {
        String stageName = "testInitializationWorker";
        IRecordProcessorFactory recordProcessorFactory = new TestStreamletFactory(null, null);
        config = new KinesisClientLibConfiguration(stageName, null, null, null);
        int count = 0;
        when(proxy.getShardList()).thenThrow(new RuntimeException(Integer.toString(count++)));
        int maxRecords = 2;
        long idleTimeInMilliseconds = 1L;
        StreamConfig streamConfig =
                new StreamConfig(proxy,
                        maxRecords,
                        idleTimeInMilliseconds,
                        callProcessRecordsForEmptyRecordList, skipCheckpointValidationValue, INITIAL_POSITION_LATEST);
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        ExecutorService execService = Executors.newSingleThreadExecutor();
        long shardPollInterval = 0L;
        Worker worker =
                new Worker(stageName,
                        recordProcessorFactory,
                        config,
                        streamConfig, INITIAL_POSITION_TRIM_HORIZON,
                        shardPollInterval,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        leaseCoordinator,
                        leaseCoordinator,
                        execService,
                        nullMetricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);
        worker.run();
        Assert.assertTrue(count > 0);
    }

    */
    /*
     * Runs worker with threadPoolSize == numShards
     * Test method for {@link Worker#run()}.
     */
    /*
    @Test
    public final void testRunWithThreadPoolSizeEqualToNumShards() throws Exception {
        final int numShards = 1;
        final int threadPoolSize = numShards;
        runAndTestWorker(numShards, threadPoolSize);
    }

    */
    /*
     * Runs worker with threadPoolSize < numShards
     * Test method for {@link Worker#run()}.
     */
    /*
    @Test
    public final void testRunWithThreadPoolSizeLessThanNumShards() throws Exception {
        final int numShards = 3;
        final int threadPoolSize = 2;
        runAndTestWorker(numShards, threadPoolSize);
    }

    */
    /*
     * Runs worker with threadPoolSize > numShards
     * Test method for {@link Worker#run()}.
     */
    /*
    @Test
    public final void testRunWithThreadPoolSizeMoreThanNumShards() throws Exception {
        final int numShards = 3;
        final int threadPoolSize = 5;
        runAndTestWorker(numShards, threadPoolSize);
    }

    */
    /*
     * Runs worker with threadPoolSize < numShards
     * Test method for {@link Worker#run()}.
     */
    /*
    @Test
    public final void testOneSplitShard2Threads() throws Exception {
        final int threadPoolSize = 2;
        final int numberOfRecordsPerShard = 10;
        List<Shard> shardList = createShardListWithOneSplit();
        List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        KinesisClientLease lease = ShardSyncer.newKCLLease(shardList.get(0));
        lease.checkpoint(new ExtendedSequenceNumber("2"));
        initialLeases.add(lease);
        runAndTestWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList, numberOfRecordsPerShard, config);
    }

    */
    /*
     * Runs worker with threadPoolSize < numShards
     * Test method for {@link Worker#run()}.
     */
    /*
    @Test
    public final void testOneSplitShard2ThreadsWithCallsForEmptyRecords() throws Exception {
        final int threadPoolSize = 2;
        final int numberOfRecordsPerShard = 10;
        List<Shard> shardList = createShardListWithOneSplit();
        List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        KinesisClientLease lease = ShardSyncer.newKCLLease(shardList.get(0));
        lease.checkpoint(new ExtendedSequenceNumber("2"));
        initialLeases.add(lease);
        boolean callProcessRecordsForEmptyRecordList = true;
        RecordsFetcherFactory recordsFetcherFactory = new SimpleRecordsFetcherFactory();
        recordsFetcherFactory.idleMillisBetweenCalls(0L);
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
        runAndTestWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList, numberOfRecordsPerShard, config);
    }

    @Test
    public final void testWorkerShutsDownOwnedResources() throws Exception {

        final long failoverTimeMillis = 20L;

        // Make sure that worker thread is run before invoking shutdown.
        final CountDownLatch workerStarted = new CountDownLatch(1);
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                workerStarted.countDown();
                return false;
            }
        }).when(executorService).isShutdown();

        final WorkerThread workerThread = runWorker(Collections.<Shard>emptyList(),
                Collections.<KinesisClientLease>emptyList(),
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                10,
                kinesisProxy, v2RecordProcessorFactory,
                executorService,
                cwMetricsFactory,
                config);

        // Give some time for thread to run.
        workerStarted.await();

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.state() == State.TERMINATED);
        verify(executorService, times(1)).shutdownNow();
        verify(cwMetricsFactory, times(1)).shutdown();
    }

    @Test
    public final void testWorkerDoesNotShutdownClientResources() throws Exception {
        final long failoverTimeMillis = 20L;

        final ExecutorService executorService = mock(ThreadPoolExecutor.class);
        final CloudWatchMetricsFactory cwMetricsFactory = mock(CloudWatchMetricsFactory.class);
        // Make sure that worker thread is run before invoking shutdown.
        final CountDownLatch workerStarted = new CountDownLatch(1);
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                workerStarted.countDown();
                return false;
            }
        }).when(executorService).isShutdown();

        final WorkerThread workerThread = runWorker(Collections.<Shard>emptyList(),
                Collections.<KinesisClientLease>emptyList(),
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                10,
                kinesisProxy, v2RecordProcessorFactory,
                executorService,
                cwMetricsFactory,
                config);

        // Give some time for thread to run.
        workerStarted.await();

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.state() == State.TERMINATED);
        verify(executorService, times(0)).shutdownNow();
        verify(cwMetricsFactory, times(0)).shutdown();
    }

    @Test
    public final void testWorkerNormalShutdown() throws Exception {
        final List<Shard> shardList = createShardListWithOneShard();
        final boolean callProcessRecordsForEmptyRecordList = true;
        final long failoverTimeMillis = 50L;
        final int numberOfRecordsPerShard = 1000;

        final List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        for (Shard shard : shardList) {
            KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            initialLeases.add(lease);
        }

        final File file = KinesisLocalFileDataCreator.generateTempDataFile(
                shardList, numberOfRecordsPerShard, "normalShutdownUnitTest");
        final IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final ExecutorService executorService = Executors.newCachedThreadPool();

        // Make test case as efficient as possible.
        final CountDownLatch processRecordsLatch = new CountDownLatch(1);

        when(v2RecordProcessorFactory.createProcessor()).thenReturn(v2RecordProcessor);

        doAnswer(new Answer<Object> () {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                // Signal that record processor has started processing records.
                processRecordsLatch.countDown();
                return null;
            }
        }).when(v2RecordProcessor).processRecords(any(ProcessRecordsInput.class));

        RecordsFetcherFactory recordsFetcherFactory = mock(RecordsFetcherFactory.class);
        RecordsPublisher getRecordsCache = mock(RecordsPublisher.class);
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
        when(recordsFetcherFactory.createRecordsFetcher(any(GetRecordsRetrievalStrategy.class), anyString(),
                any(IMetricsFactory.class), anyInt()))
                .thenReturn(getRecordsCache);
        when(getRecordsCache.getNextResult()).thenReturn(new ProcessRecordsInput().records(Collections.emptyList()).millisBehindLatest(0L));

        WorkerThread workerThread = runWorker(shardList,
                initialLeases,
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                numberOfRecordsPerShard,
                fileBasedProxy,
                v2RecordProcessorFactory,
                executorService,
                nullMetricsFactory,
                config);

        // Only sleep for time that is required.
        processRecordsLatch.await();

        // Make sure record processor is initialized and processing records.
        verify(v2RecordProcessorFactory, times(1)).createProcessor();
        verify(v2RecordProcessor, times(1)).initialize(any(InitializationInput.class));
        verify(v2RecordProcessor, atLeast(1)).processRecords(any(ProcessRecordsInput.class));
        verify(v2RecordProcessor, times(0)).shutdown(any(ShutdownInput.class));

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.state() == State.TERMINATED);
        verify(v2RecordProcessor, times(1)).shutdown(any(ShutdownInput.class));
    }

    */
    /*
     * This test is testing the {@link Worker}'s shutdown behavior and by extension the behavior of
     * {@link ThreadPoolExecutor#shutdownNow()}. It depends on the thread pool sending an interrupt to the pool threads.
     * This behavior makes the test a bit racy, since we need to ensure a specific order of events.
     *
     * @throws Exception
     */
    /*
    @Test
    public final void testWorkerForcefulShutdown() throws Exception {
        final List<Shard> shardList = createShardListWithOneShard();
        final boolean callProcessRecordsForEmptyRecordList = true;
        final long failoverTimeMillis = 50L;
        final int numberOfRecordsPerShard = 10;

        final List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        for (Shard shard : shardList) {
            KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
            lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
            initialLeases.add(lease);
        }

        final File file = KinesisLocalFileDataCreator.generateTempDataFile(
                shardList, numberOfRecordsPerShard, "normalShutdownUnitTest");
        final IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        // Get executor service that will be owned by the worker, so we can get interrupts.
        ExecutorService executorService = getWorkerThreadPoolExecutor();

        // Make test case as efficient as possible.
        final CountDownLatch processRecordsLatch = new CountDownLatch(1);
        final AtomicBoolean recordProcessorInterrupted = new AtomicBoolean(false);
        when(v2RecordProcessorFactory.createProcessor()).thenReturn(v2RecordProcessor);
        final Semaphore actionBlocker = new Semaphore(1);
        final Semaphore shutdownBlocker = new Semaphore(1);

        actionBlocker.acquire();

        doAnswer(new Answer<Object> () {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                // Signal that record processor has started processing records.
                processRecordsLatch.countDown();

                // Block for some time now to test forceful shutdown. Also, check if record processor
                // was interrupted or not.
                final long startTimeMillis = System.currentTimeMillis();
                long elapsedTimeMillis = 0;

                log.info("Entering sleep @ {} with elapsedMills: {}", startTimeMillis, elapsedTimeMillis);
                shutdownBlocker.acquire();
                try {
                    actionBlocker.acquire();
                } catch (InterruptedException e) {
                    log.info("Sleep interrupted @ {} elapsedMillis: {}", System.currentTimeMillis(),
                            (System.currentTimeMillis() - startTimeMillis));
                    recordProcessorInterrupted.getAndSet(true);
                }
                shutdownBlocker.release();
                elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;
                log.info("Sleep completed @ {} elapsedMillis: {}", System.currentTimeMillis(), elapsedTimeMillis);

                return null;
            }
        }).when(v2RecordProcessor).processRecords(any(ProcessRecordsInput.class));

        WorkerThread workerThread = runWorker(shardList,
                initialLeases,
                callProcessRecordsForEmptyRecordList,
                failoverTimeMillis,
                numberOfRecordsPerShard,
                fileBasedProxy,
                v2RecordProcessorFactory,
                executorService,
                nullMetricsFactory,
                config);

        // Only sleep for time that is required.
        processRecordsLatch.await();

        // Make sure record processor is initialized and processing records.
        verify(v2RecordProcessorFactory, times(1)).createProcessor();
        verify(v2RecordProcessor, times(1)).initialize(any(InitializationInput.class));
        verify(v2RecordProcessor, atLeast(1)).processRecords(any(ProcessRecordsInput.class));
        verify(v2RecordProcessor, times(0)).shutdown(any(ShutdownInput.class));

        workerThread.getWorker().shutdown();
        workerThread.join();

        Assert.assertTrue(workerThread.state() == State.TERMINATED);
        // Shutdown should not be called in this case because record processor is blocked.
        verify(v2RecordProcessor, times(0)).shutdown(any(ShutdownInput.class));

        //
        // Release the worker thread
        //
        actionBlocker.release();
        //
        // Give the worker thread time to execute it's interrupted handler.
        //
        shutdownBlocker.tryAcquire(100, TimeUnit.MILLISECONDS);
        //
        // Now we can see if it was actually interrupted. It's possible it wasn't and this will fail.
        //
        assertThat(recordProcessorInterrupted.get(), equalTo(true));
    }

    @Test
    public void testRequestShutdown() throws Exception {


        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().checkpoint(checkpoint)
                .concurrencyToken(UUID.randomUUID()).lastCounterIncrementNanos(0L).leaseCounter(0L)
                .ownerSwitchesSinceCheckpoint(0L).leaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.leaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.leaseKey(), lease.concurrencyToken().toString(),
                lease.parentShardIds(), lease.checkpoint()));


        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);


        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        worker.createWorkerShutdownCallable().call();
        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        worker.runProcessLoop();

        verify(leaseCoordinator, atLeastOnce()).dropLease(eq(lease));
        leases.clear();
        currentAssignments.clear();

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

    }

    @Test(expected = IllegalStateException.class)
    public void testShutdownCallableNotAllowedTwice() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().checkpoint(checkpoint)
                .concurrencyToken(UUID.randomUUID()).lastCounterIncrementNanos(0L).leaseCounter(0L)
                .ownerSwitchesSinceCheckpoint(0L).leaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.leaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.leaseKey(), lease.concurrencyToken().toString(),
                lease.parentShardIds(), lease.checkpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new InjectableWorker("testRequestShutdown", recordProcessorFactory, config, streamConfig,
                INITIAL_POSITION_TRIM_HORIZON, parentShardPollIntervalMillis, shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion, leaseCoordinator, leaseCoordinator, executorService, metricsFactory,
                taskBackoffTimeMillis, failoverTimeMillis, false, shardPrioritization) {
            @Override
            void postConstruct() {
                this.gracefuleShutdownStarted = true;
            }
        };

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        assertThat(worker.hasGracefulShutdownStarted(), equalTo(true));
        worker.createWorkerShutdownCallable().call();

    }

    @Test
    public void testGracefulShutdownSingleFuture() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().checkpoint(checkpoint)
                .concurrencyToken(UUID.randomUUID()).lastCounterIncrementNanos(0L).leaseCounter(0L)
                .ownerSwitchesSinceCheckpoint(0L).leaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.leaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.leaseKey(), lease.concurrencyToken().toString(),
                lease.parentShardIds(), lease.checkpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        GracefulShutdownCoordinator coordinator = mock(GracefulShutdownCoordinator.class);
        when(coordinator.createGracefulShutdownCallable(any(Callable.class))).thenReturn(() -> true);

        Future<Boolean> gracefulShutdownFuture = mock(Future.class);

        when(coordinator.startGracefulShutdown(any(Callable.class))).thenReturn(gracefulShutdownFuture);

        Worker worker = new InjectableWorker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization) {
            @Override
            void postConstruct() {
                this.gracefulShutdownCoordinator = coordinator;
            }
        };

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        Future<Boolean> firstFuture = worker.startGracefulShutdown();
        Future<Boolean> secondFuture = worker.startGracefulShutdown();

        assertThat(firstFuture, equalTo(secondFuture));
        verify(coordinator).startGracefulShutdown(any(Callable.class));

    }

    @Test
    public void testRequestShutdownNoLeases() throws Exception {


        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);


        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);


        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        worker.createWorkerShutdownCallable().call();
        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        worker.runProcessLoop();
        verify(executorService, never()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

        assertThat(worker.shouldShutdown(), equalTo(true));

    }

    @Test
    public void testRequestShutdownWithLostLease() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLease lease1 = makeLease(checkpoint, 1);
        KinesisClientLease lease2 = makeLease(checkpoint, 2);
        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        leases.add(lease1);
        leases.add(lease2);

        ShardInfo shardInfo1 = makeShardInfo(lease1);
        currentAssignments.add(shardInfo1);
        ShardInfo shardInfo2 = makeShardInfo(lease2);
        currentAssignments.add(shardInfo2);

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.getShardInfoShardConsumerMap().remove(shardInfo2);
        worker.createWorkerShutdownCallable().call();
        leases.remove(1);
        currentAssignments.remove(1);
        worker.runProcessLoop();


        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(leaseCoordinator).dropLease(eq(lease1));
        verify(leaseCoordinator, never()).dropLease(eq(lease2));
        leases.clear();
        currentAssignments.clear();

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo2)))));

    }

    @Test
    public void testRequestShutdownWithAllLeasesLost() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLease lease1 = makeLease(checkpoint, 1);
        KinesisClientLease lease2 = makeLease(checkpoint, 2);
        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        leases.add(lease1);
        leases.add(lease2);

        ShardInfo shardInfo1 = makeShardInfo(lease1);
        currentAssignments.add(shardInfo1);
        ShardInfo shardInfo2 = makeShardInfo(lease2);
        currentAssignments.add(shardInfo2);

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS)).and(ReflectionFieldMatcher
                        .withField(BlockOnParentShardTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo1)))));
        verify(executorService).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE)).and(ReflectionFieldMatcher
                        .withField(InitializeTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.getShardInfoShardConsumerMap().clear();
        Future<Void> future = worker.requestShutdown();

        leases.clear();
        currentAssignments.clear();

        try {
            future.get(1, TimeUnit.HOURS);
        } catch (TimeoutException te) {
            fail("Future from requestShutdown should immediately return.");
        }

        worker.runProcessLoop();
        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION)).and(ReflectionFieldMatcher
                        .withField(ShutdownNotificationTask.class, "shardInfo", equalTo(shardInfo2)))));

        worker.runProcessLoop();

        verify(leaseCoordinator, never()).dropLease(eq(lease1));
        verify(leaseCoordinator, never()).dropLease(eq(lease2));

        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo1)))));

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN)).and(ReflectionFieldMatcher
                        .withField(ShutdownTask.class, "shardInfo", equalTo(shardInfo2)))));



        assertThat(worker.shouldShutdown(), equalTo(true));

    }

    @Test
    public void testLeaseCancelledAfterShutdownRequest() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().checkpoint(checkpoint)
                .concurrencyToken(UUID.randomUUID()).lastCounterIncrementNanos(0L).leaseCounter(0L)
                .ownerSwitchesSinceCheckpoint(0L).leaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.leaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.leaseKey(), lease.concurrencyToken().toString(),
                lease.parentShardIds(), lease.checkpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));

        worker.requestShutdown();
        leases.clear();
        currentAssignments.clear();
        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce())
                .submit(argThat(ShutdownReasonMatcher.hasReason(equalTo(ShutdownReason.ZOMBIE))));
        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

    }

    @Test
    public void testEndOfShardAfterShutdownRequest() throws Exception {

        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        StreamConfig streamConfig = mock(StreamConfig.class);
        IMetricsFactory metricsFactory = mock(IMetricsFactory.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().checkpoint(checkpoint)
                .concurrencyToken(UUID.randomUUID()).lastCounterIncrementNanos(0L).leaseCounter(0L)
                .ownerSwitchesSinceCheckpoint(0L).leaseOwner("Self");

        final List<KinesisClientLease> leases = new ArrayList<>();
        final List<ShardInfo> currentAssignments = new ArrayList<>();
        KinesisClientLease lease = builder.leaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);
        currentAssignments.add(new ShardInfo(lease.leaseKey(), lease.concurrencyToken().toString(),
                lease.parentShardIds(), lease.checkpoint()));

        when(leaseCoordinator.getAssignments()).thenAnswer(new Answer<List<KinesisClientLease>>() {
            @Override
            public List<KinesisClientLease> answer(InvocationOnMock invocation) throws Throwable {
                return leases;
            }
        });
        when(leaseCoordinator.getCurrentAssignments()).thenAnswer(new Answer<List<ShardInfo>>() {
            @Override
            public List<ShardInfo> answer(InvocationOnMock invocation) throws Throwable {
                return currentAssignments;
            }
        });

        IRecordProcessor processor = mock(IRecordProcessor.class);
        when(recordProcessorFactory.createProcessor()).thenReturn(processor);

        Worker worker = new Worker("testRequestShutdown",
                recordProcessorFactory,
                config,
                streamConfig,
                INITIAL_POSITION_TRIM_HORIZON,
                parentShardPollIntervalMillis,
                shardSyncIntervalMillis,
                cleanupLeasesUponShardCompletion,
                leaseCoordinator,
                leaseCoordinator,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                failoverTimeMillis,
                false,
                shardPrioritization);

        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.BLOCK_ON_PARENT_SHARDS))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.INITIALIZE))));
        when(taskResult.isShardEndReached()).thenReturn(true);

        worker.requestShutdown();
        worker.runProcessLoop();

        verify(executorService, never()).submit(argThat(both(isA(MetricsCollectingTaskDecorator.class))
                .and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN_NOTIFICATION))));

        verify(executorService).submit(argThat(ShutdownReasonMatcher.hasReason(equalTo(ShutdownReason.TERMINATE))));

        worker.runProcessLoop();

        verify(executorService, atLeastOnce()).submit(argThat(
                both(isA(MetricsCollectingTaskDecorator.class)).and(TaskTypeMatcher.isOfType(TaskType.SHUTDOWN))));

    }

    @Test
    public void testBuilderWithDefaultKinesisProxy() {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        Worker worker = new Worker.Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .build();
        Assert.assertNotNull(worker.getStreamConfig().getStreamProxy());
        Assert.assertTrue(worker.getStreamConfig().getStreamProxy() instanceof KinesisProxy);
    }

    @Test
    public void testBuilderWhenKinesisProxyIsSet() {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        // Create an instance of KinesisLocalFileProxy for injection and validation
        IKinesisProxy kinesisProxy = mock(KinesisLocalFileProxy.class);
        Worker worker = new Worker.Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisProxy(kinesisProxy)
            .build();
        Assert.assertNotNull(worker.getStreamConfig().getStreamProxy());
        Assert.assertTrue(worker.getStreamConfig().getStreamProxy() instanceof KinesisLocalFileProxy);
    }

    @Test
    public void testBuilderForWorkerStateListener() {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .build();
        Assert.assertTrue(worker.getWorkerStateChangeListener() instanceof NoOpWorkerStateChangeListener);
    }

    @Test
    public void testBuilderWhenWorkerStateListenerIsSet() {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .workerStateChangeListener(workerStateChangeListener)
                .config(config)
                .build();
        Assert.assertSame(workerStateChangeListener, worker.getWorkerStateChangeListener());
    }

    @Test
    public void testWorkerStateListenerStatePassesThroughCreatedState() {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .workerStateChangeListener(workerStateChangeListener)
                .config(config)
                .build();

        verify(workerStateChangeListener, times(1)).onWorkerStateChange(eq(WorkerState.CREATED));
    }

    @Test
    @Ignore
    public void testWorkerStateChangeListenerGoesThroughStates() throws Exception {

        final CountDownLatch workerInitialized = new CountDownLatch(1);
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        final IRecordProcessor processor = mock(IRecordProcessor.class);

        ExtendedSequenceNumber checkpoint = new ExtendedSequenceNumber("123", 0L);
        KinesisClientLeaseBuilder builder = new KinesisClientLeaseBuilder().checkpoint(checkpoint)
                .concurrencyToken(UUID.randomUUID()).lastCounterIncrementNanos(0L).leaseCounter(0L)
                .ownerSwitchesSinceCheckpoint(0L).leaseOwner("Self");
        final List<KinesisClientLease> leases = new ArrayList<>();
        KinesisClientLease lease = builder.leaseKey(String.format("shardId-%03d", 1)).build();
        leases.add(lease);

        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                workerInitialized.countDown();
                return true;
            }
        }).when(leaseRefresher).waitUntilLeaseTableExists(anyLong(), anyLong());
        doAnswer(new Answer<IRecordProcessor>() {
            @Override
            public IRecordProcessor answer(InvocationOnMock invocation) throws Throwable {
                workerStarted.countDown();
                return processor;
            }
        }).when(recordProcessorFactory).createProcessor();

        when(config.workerIdentifier()).thenReturn("Self");
        when(leaseRefresher.listLeases()).thenReturn(leases);
        when(leaseRefresher.renewLease(leases.get(0))).thenReturn(true);
        when(executorService.submit(Matchers.<Callable<TaskResult>> any()))
                .thenAnswer(new ShutdownHandlingAnswer(taskFuture));
        when(taskFuture.isDone()).thenReturn(true);
        when(taskFuture.get()).thenReturn(taskResult);
        when(taskResult.isShardEndReached()).thenReturn(true);

        Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .leaseRefresher(leaseRefresher)
                .kinesisProxy(kinesisProxy)
                .execService(executorService)
                .workerStateChangeListener(workerStateChangeListener)
                .build();

        verify(workerStateChangeListener, times(1)).onWorkerStateChange(eq(WorkerState.CREATED));

        WorkerThread workerThread = new WorkerThread(worker);
        workerThread.start();

        workerInitialized.await();
        verify(workerStateChangeListener, times(1)).onWorkerStateChange(eq(WorkerState.INITIALIZING));

        workerStarted.await();
        verify(workerStateChangeListener, times(1)).onWorkerStateChange(eq(WorkerState.STARTED));

        boolean workerShutdown = worker.createGracefulShutdownCallable()
                .call();

        verify(workerStateChangeListener, times(1)).onWorkerStateChange(eq(WorkerState.SHUT_DOWN));
    }

    @Test
    public void testBuilderWithDefaultLeaseManager()  {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);

        Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .build();

        Assert.assertNotNull(worker.getLeaseCoordinator().leaseRefresher());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBuilderWhenLeaseManagerIsSet()  {
        IRecordProcessorFactory recordProcessorFactory = mock(IRecordProcessorFactory.class);
        // Create an instance of ILeaseManager for injection and validation
        ILeaseManager<KinesisClientLease> leaseRefresher = (ILeaseManager<KinesisClientLease>) mock(ILeaseManager.class);
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(config)
                .leaseRefresher(leaseRefresher)
                .build();

        Assert.assertSame(leaseRefresher, worker.getLeaseCoordinator().leaseRefresher());
    }

    private abstract class InjectableWorker extends Worker {
        InjectableWorker(String applicationName, IRecordProcessorFactory recordProcessorFactory,
                KinesisClientLibConfiguration config, StreamConfig streamConfig,
                InitialPositionInStreamExtended initialPositionInStream,
                long parentShardPollIntervalMillis, long shardSyncIdleTimeMillis,
                boolean cleanupLeasesUponShardCompletion, ICheckpoint checkpoint,
                KinesisClientLibLeaseCoordinator leaseCoordinator, ExecutorService execService,
                IMetricsFactory metricsFactory, long taskBackoffTimeMillis, long failoverTimeMillis,
                boolean skipShardSyncAtWorkerInitializationIfLeasesExist, ShardPrioritization shardPrioritization) {
            super(applicationName,
                    recordProcessorFactory,
                    config,
                    streamConfig,
                    initialPositionInStream,
                    parentShardPollIntervalMillis,
                    shardSyncIdleTimeMillis,
                    cleanupLeasesUponShardCompletion,
                    checkpoint,
                    leaseCoordinator,
                    execService,
                    metricsFactory,
                    taskBackoffTimeMillis,
                    failoverTimeMillis,
                    skipShardSyncAtWorkerInitializationIfLeasesExist,
                    shardPrioritization);
            postConstruct();
        }

        abstract void postConstruct();
    }

    private KinesisClientLease makeLease(ExtendedSequenceNumber checkpoint, int shardId) {
        return new KinesisClientLeaseBuilder().checkpoint(checkpoint).concurrencyToken(UUID.randomUUID())
                .lastCounterIncrementNanos(0L).leaseCounter(0L).ownerSwitchesSinceCheckpoint(0L)
                .leaseOwner("Self").leaseKey(String.format("shardId-%03d", shardId)).build();
    }

    private ShardInfo makeShardInfo(KinesisClientLease lease) {
        return new ShardInfo(lease.leaseKey(), lease.concurrencyToken().toString(), lease.parentShardIds(),
                lease.checkpoint());
    }

    private static class ShutdownReasonMatcher extends TypeSafeDiagnosingMatcher<MetricsCollectingTaskDecorator> {

        private final Matcher<ShutdownReason> matcher;

        public ShutdownReasonMatcher(Matcher<ShutdownReason> matcher) {
            this.matcher = matcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("hasShutdownReason(").appendDescriptionOf(matcher).appendText(")");
        }

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item, Description mismatchDescription) {
            return Condition.matched(item, mismatchDescription)
                    .and(new Condition.Step<MetricsCollectingTaskDecorator, ShutdownTask>() {
                        @Override
                        public Condition<ShutdownTask> apply(MetricsCollectingTaskDecorator value,
                                Description mismatch) {
                            if (!(value.getOther() instanceof ShutdownTask)) {
                                mismatch.appendText("Wrapped task isn't a shutdown task");
                                return Condition.notMatched();
                            }
                            return Condition.matched((ShutdownTask) value.getOther(), mismatch);
                        }
                    }).and(new Condition.Step<ShutdownTask, ShutdownReason>() {
                        @Override
                        public Condition<ShutdownReason> apply(ShutdownTask value, Description mismatch) {
                            return Condition.matched(value.getReason(), mismatch);
                        }
                    }).matching(matcher);
        }

        public static ShutdownReasonMatcher hasReason(Matcher<ShutdownReason> matcher) {
            return new ShutdownReasonMatcher(matcher);
        }
    }

    private static class ShutdownHandlingAnswer implements Answer<Future<TaskResult>> {

        final Future<TaskResult> defaultFuture;

        public ShutdownHandlingAnswer(Future<TaskResult> defaultFuture) {
            this.defaultFuture = defaultFuture;
        }

        @Override
        public Future<TaskResult> answer(InvocationOnMock invocation) throws Throwable {
            ConsumerTask rootTask = (ConsumerTask) invocation.getArguments()[0];
            if (rootTask instanceof MetricsCollectingTaskDecorator
                    && ((MetricsCollectingTaskDecorator) rootTask).getOther() instanceof ShutdownNotificationTask) {
                ShutdownNotificationTask task = (ShutdownNotificationTask) ((MetricsCollectingTaskDecorator) rootTask).getOther();
                return Futures.immediateFuture(task.call());
            }
            return defaultFuture;
        }
    }

    private static class TaskTypeMatcher extends TypeSafeMatcher<MetricsCollectingTaskDecorator> {

        final Matcher<TaskType> expectedTaskType;

        TaskTypeMatcher(TaskType expectedTaskType) {
            this(equalTo(expectedTaskType));
        }

        TaskTypeMatcher(Matcher<TaskType> expectedTaskType) {
            this.expectedTaskType = expectedTaskType;
        }

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item) {
            return expectedTaskType.matches(item.taskType());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("taskType matches");
            expectedTaskType.describeTo(description);
        }

        static TaskTypeMatcher isOfType(TaskType taskType) {
            return new TaskTypeMatcher(taskType);
        }

        static TaskTypeMatcher matchesType(Matcher<TaskType> matcher) {
            return new TaskTypeMatcher(matcher);
        }
    }

    private static class InnerTaskMatcher<T extends ConsumerTask> extends TypeSafeMatcher<MetricsCollectingTaskDecorator> {

        final Matcher<?> matcher;

        InnerTaskMatcher(Matcher<?> matcher) {
            this.matcher = matcher;
        }

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item) {
            return matcher.matches(item.getOther());
        }

        @Override
        public void describeTo(Description description) {
            matcher.describeTo(description);
        }

        static <U extends ConsumerTask> InnerTaskMatcher<U> taskWith(Class<U> clazz, Matcher<?> matcher) {
            return new InnerTaskMatcher<>(matcher);
        }
    }

    @RequiredArgsConstructor
    private static class ReflectionFieldMatcher<T extends ConsumerTask>
            extends TypeSafeDiagnosingMatcher<MetricsCollectingTaskDecorator> {

        private final Class<T> itemClass;
        private final String fieldName;
        private final Matcher<?> fieldMatcher;

        @Override
        protected boolean matchesSafely(MetricsCollectingTaskDecorator item, Description mismatchDescription) {
            if (item.getOther() == null) {
                mismatchDescription.appendText("inner task is null");
                return false;
            }
            ConsumerTask inner = item.getOther();
            if (!itemClass.equals(inner.getClass())) {
                mismatchDescription.appendText("inner task isn't an instance of ").appendText(itemClass.getName());
                return false;
            }
            try {
                Field field = itemClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                if (!fieldMatcher.matches(field.get(inner))) {
                    mismatchDescription.appendText("Field '").appendText(fieldName).appendText("' doesn't match: ")
                            .appendDescriptionOf(fieldMatcher);
                    return false;
                }
                return true;
            } catch (NoSuchFieldException e) {
                mismatchDescription.appendText(itemClass.getName()).appendText(" doesn't have a field named ")
                        .appendText(fieldName);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("An item of ").appendText(itemClass.getName()).appendText(" with the field '")
                    .appendText(fieldName).appendText("' matching ").appendDescriptionOf(fieldMatcher);
        }

        static <T extends ConsumerTask> ReflectionFieldMatcher<T> withField(Class<T> itemClass, String fieldName,
                Matcher<?> fieldMatcher) {
            return new ReflectionFieldMatcher<>(itemClass, fieldName, fieldMatcher);
        }
    }

    */
    /*
     * Returns executor service that will be owned by the worker. This is useful to test the scenario
     * where worker shuts down the executor service also during shutdown flow.
     *
     * @return Executor service that will be owned by the worker.
     */
    /*
    private WorkerThreadPoolExecutor getWorkerThreadPoolExecutor() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("ShardRecordProcessor-%04d").build();
        return new WorkerThreadPoolExecutor(threadFactory);
    }

    private List<Shard> createShardListWithOneShard() {
        List<Shard> shards = new ArrayList<Shard>();
        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("39428", "987324");
        HashKeyRange keyRange =
                ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, ShardObjectHelper.MAX_HASH_KEY);
        Shard shard0 = ShardObjectHelper.newShard("shardId-0", null, null, range0, keyRange);
        shards.add(shard0);

        return shards;
    }

    private List<Shard> createShardListWithOneSplit() {
        List<Shard> shards = new ArrayList<Shard>();
        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("39428", "987324");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("987325", null);
        HashKeyRange keyRange =
                ShardObjectHelper.newHashKeyRange(ShardObjectHelper.MIN_HASH_KEY, ShardObjectHelper.MAX_HASH_KEY);
        Shard shard0 = ShardObjectHelper.newShard("shardId-0", null, null, range0, keyRange);
        shards.add(shard0);

        Shard shard1 = ShardObjectHelper.newShard("shardId-1", "shardId-0", null, range1, keyRange);
        shards.add(shard1);

        return shards;
    }

    private void runAndTestWorker(int numShards, int threadPoolSize) throws Exception {
        final int numberOfRecordsPerShard = 10;
        final String kinesisShardPrefix = "kinesis-0-";
        final BigInteger startSeqNum = BigInteger.ONE;
        List<Shard> shardList = KinesisLocalFileDataCreator.createShardList(numShards, kinesisShardPrefix, startSeqNum);
        Assert.assertEquals(numShards, shardList.size());
        List<KinesisClientLease> initialLeases = new ArrayList<KinesisClientLease>();
        for (Shard shard : shardList) {
            KinesisClientLease lease = ShardSyncer.newKCLLease(shard);
            lease.checkpoint(ExtendedSequenceNumber.AT_TIMESTAMP);
            initialLeases.add(lease);
        }
        runAndTestWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList, numberOfRecordsPerShard, config);
    }

    private void runAndTestWorker(List<Shard> shardList,
                                  int threadPoolSize,
                                  List<KinesisClientLease> initialLeases,
                                  boolean callProcessRecordsForEmptyRecordList,
                                  int numberOfRecordsPerShard,
                                  KinesisClientLibConfiguration clientConfig) throws Exception {
        File file = KinesisLocalFileDataCreator.generateTempDataFile(shardList, numberOfRecordsPerShard, "unitTestWT001");
        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        Semaphore recordCounter = new Semaphore(0);
        ShardSequenceVerifier shardSequenceVerifier = new ShardSequenceVerifier(shardList);
        TestStreamletFactory recordProcessorFactory = new TestStreamletFactory(recordCounter, shardSequenceVerifier);

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        WorkerThread workerThread = runWorker(
                shardList, initialLeases, callProcessRecordsForEmptyRecordList, failoverTimeMillis,
                numberOfRecordsPerShard, fileBasedProxy, recordProcessorFactory, executorService, nullMetricsFactory, clientConfig);

        // TestStreamlet will release the semaphore once for every record it processes
        recordCounter.acquire(numberOfRecordsPerShard * shardList.size());

        // Wait a bit to allow the worker to spin against the end of the stream.
        Thread.sleep(500L);

        testWorker(shardList, threadPoolSize, initialLeases, callProcessRecordsForEmptyRecordList,
                numberOfRecordsPerShard, fileBasedProxy, recordProcessorFactory);

        workerThread.getWorker().shutdown();
        executorService.shutdownNow();
        file.delete();
    }

    private WorkerThread runWorker(List<Shard> shardList,
                                   List<KinesisClientLease> initialLeases,
                                   boolean callProcessRecordsForEmptyRecordList,
                                   long failoverTimeMillis,
                                   int numberOfRecordsPerShard,
                                   IKinesisProxy kinesisProxy,
                                   IRecordProcessorFactory recordProcessorFactory,
                                   ExecutorService executorService,
                                   IMetricsFactory metricsFactory,
                                   KinesisClientLibConfiguration clientConfig) throws Exception {
        final String stageName = "testStageName";
        final int maxRecords = 2;

        final long leaseDurationMillis = 10000L;
        final long epsilonMillis = 1000L;
        final long idleTimeInMilliseconds = 2L;

        AmazonDynamoDB ddbClient = DynamoDBEmbedded.create().amazonDynamoDB();
        LeaseManager<KinesisClientLease> leaseRefresher = new KinesisClientLeaseManager("foo", ddbClient);
        leaseRefresher.createLeaseTableIfNotExists(1L, 1L);
        for (KinesisClientLease initialLease : initialLeases) {
            leaseRefresher.createLeaseIfNotExists(initialLease);
        }

        KinesisClientLibLeaseCoordinator leaseCoordinator =
                new KinesisClientLibLeaseCoordinator(leaseRefresher,
                        stageName,
                        leaseDurationMillis,
                        epsilonMillis,
                        metricsFactory);

        final Date timestamp = new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP);
        StreamConfig streamConfig = new StreamConfig(kinesisProxy,
                maxRecords,
                idleTimeInMilliseconds,
                callProcessRecordsForEmptyRecordList,
                skipCheckpointValidationValue, InitialPositionInStreamExtended.newInitialPositionAtTimestamp(timestamp));

        Worker worker =
                new Worker(stageName,
                        recordProcessorFactory,
                        clientConfig,
                        streamConfig, INITIAL_POSITION_TRIM_HORIZON,
                        parentShardPollIntervalMillis,
                        shardSyncIntervalMillis,
                        cleanupLeasesUponShardCompletion,
                        leaseCoordinator,
                        leaseCoordinator,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        failoverTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        shardPrioritization);

        WorkerThread workerThread = new WorkerThread(worker);
        workerThread.start();
        return workerThread;
    }

    private void testWorker(List<Shard> shardList,
            int threadPoolSize,
            List<KinesisClientLease> initialLeases,
            boolean callProcessRecordsForEmptyRecordList,
            int numberOfRecordsPerShard,
            IKinesisProxy kinesisProxy,
            TestStreamletFactory recordProcessorFactory) throws Exception {
        recordProcessorFactory.getShardSequenceVerifier().verify();

        // Gather values to compare across all processors of a given shard.
        Map<String, List<Record>> shardStreamletsRecords = new HashMap<String, List<Record>>();
        Map<String, ShutdownReason> shardsLastProcessorShutdownReason = new HashMap<String, ShutdownReason>();
        Map<String, Long> shardsNumProcessRecordsCallsWithEmptyRecordList = new HashMap<String, Long>();
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            String shardId = processor.shardId();
            if (shardStreamletsRecords.get(shardId) == null) {
                shardStreamletsRecords.put(shardId, processor.getProcessedRecords());
            } else {
                List<Record> records = shardStreamletsRecords.get(shardId);
                records.addAll(processor.getProcessedRecords());
                shardStreamletsRecords.put(shardId, records);
            }
            if (shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId) == null) {
                shardsNumProcessRecordsCallsWithEmptyRecordList.put(shardId,
                        processor.getNumProcessRecordsCallsWithEmptyRecordList());
            } else {
                long totalShardsNumProcessRecordsCallsWithEmptyRecordList =
                        shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId)
                                + processor.getNumProcessRecordsCallsWithEmptyRecordList();
                shardsNumProcessRecordsCallsWithEmptyRecordList.put(shardId,
                        totalShardsNumProcessRecordsCallsWithEmptyRecordList);
            }
            shardsLastProcessorShutdownReason.put(processor.shardId(), processor.getShutdownReason());
        }

        // verify that all records were processed at least once
        verifyAllRecordsOfEachShardWereConsumedAtLeastOnce(shardList, kinesisProxy, numberOfRecordsPerShard, shardStreamletsRecords);

        // within a record processor all the incoming records should be ordered
        verifyRecordsProcessedByEachProcessorWereOrdered(recordProcessorFactory);

        // for shards for which only one record processor was created, we verify that each record should be
        // processed exactly once
        verifyAllRecordsOfEachShardWithOnlyOneProcessorWereConsumedExactlyOnce(shardList,
                kinesisProxy,
                numberOfRecordsPerShard,
                shardStreamletsRecords,
                recordProcessorFactory);

        // if callProcessRecordsForEmptyRecordList flag is set then processors must have been invoked with empty record
        // sets else they shouldn't have seen invoked with empty record sets
        verifyNumProcessRecordsCallsWithEmptyRecordList(shardList,
                shardsNumProcessRecordsCallsWithEmptyRecordList,
                callProcessRecordsForEmptyRecordList);

        // verify that worker shutdown last processor of shards that were terminated
        verifyLastProcessorOfClosedShardsWasShutdownWithTerminate(shardList, shardsLastProcessorShutdownReason);
    }

    // within a record processor all the incoming records should be ordered
    private void verifyRecordsProcessedByEachProcessorWereOrdered(TestStreamletFactory recordProcessorFactory) {
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            List<Record> processedRecords = processor.getProcessedRecords();
            for (int i = 0; i < processedRecords.size() - 1; i++) {
                BigInteger sequenceNumberOfcurrentRecord = new BigInteger(processedRecords.get(i).sequenceNumber());
                BigInteger sequenceNumberOfNextRecord = new BigInteger(processedRecords.get(i + 1).sequenceNumber());
                Assert.assertTrue(sequenceNumberOfcurrentRecord.subtract(sequenceNumberOfNextRecord).signum() == -1);
            }
        }
    }

    // for shards for which only one record processor was created, we verify that each record should be
    // processed exactly once
    private void verifyAllRecordsOfEachShardWithOnlyOneProcessorWereConsumedExactlyOnce(List<Shard> shardList,
            IKinesisProxy fileBasedProxy,
            int numRecs,
            Map<String, List<Record>> shardStreamletsRecords,
            TestStreamletFactory recordProcessorFactory) {
        Map<String, TestStreamlet> shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor =
                findShardIdsAndStreamLetsOfShardsWithOnlyOneProcessor(recordProcessorFactory);
        for (Shard shard : shardList) {
            String shardId = shard.shardId();
            String iterator =
                    fileBasedProxy.getIterator(shardId, new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            List<Record> expectedRecords = fileBasedProxy.get(iterator, numRecs).records();
            if (shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.containsKey(shardId)) {
                verifyAllRecordsWereConsumedExactlyOnce(expectedRecords,
                        shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.get(shardId).getProcessedRecords());
            }
        }
    }

    // verify that all records were processed at least once
    private void verifyAllRecordsOfEachShardWereConsumedAtLeastOnce(List<Shard> shardList,
            IKinesisProxy fileBasedProxy,
            int numRecs,
            Map<String, List<Record>> shardStreamletsRecords) {
        for (Shard shard : shardList) {
            String shardId = shard.shardId();
            String iterator =
                    fileBasedProxy.getIterator(shardId, new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            List<Record> expectedRecords = fileBasedProxy.get(iterator, numRecs).records();
            verifyAllRecordsWereConsumedAtLeastOnce(expectedRecords, shardStreamletsRecords.get(shardId));
        }

    }

    // verify that worker shutdown last processor of shards that were terminated
    private void verifyLastProcessorOfClosedShardsWasShutdownWithTerminate(List<Shard> shardList,
            Map<String, ShutdownReason> shardsLastProcessorShutdownReason) {
        for (Shard shard : shardList) {
            String shardId = shard.shardId();
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            if (endingSequenceNumber != null) {
                log.info("Closed shard {} has an endingSequenceNumber {}", shardId, endingSequenceNumber);
                Assert.assertEquals(ShutdownReason.TERMINATE, shardsLastProcessorShutdownReason.get(shardId));
            }
        }
    }

    // if callProcessRecordsForEmptyRecordList flag is set then processors must have been invoked with empty record
    // sets else they shouldn't have seen invoked with empty record sets
    private void verifyNumProcessRecordsCallsWithEmptyRecordList(List<Shard> shardList,
            Map<String, Long> shardsNumProcessRecordsCallsWithEmptyRecordList,
            boolean callProcessRecordsForEmptyRecordList) {
        for (Shard shard : shardList) {
            String shardId = shard.shardId();
            String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
            // check only for open shards
            if (endingSequenceNumber == null) {
                if (callProcessRecordsForEmptyRecordList) {
                    Assert.assertTrue(shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId) > 0);
                } else {
                    Assert.assertEquals(0, (long) shardsNumProcessRecordsCallsWithEmptyRecordList.get(shardId));
                }
            }
        }
    }

    private Map<String, TestStreamlet>
            findShardIdsAndStreamLetsOfShardsWithOnlyOneProcessor(TestStreamletFactory recordProcessorFactory) {
        Map<String, TestStreamlet> shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor =
                new HashMap<String, TestStreamlet>();
        Set<String> seenShardIds = new HashSet<String>();
        for (TestStreamlet processor : recordProcessorFactory.getTestStreamlets()) {
            String shardId = processor.shardId();
            if (seenShardIds.add(shardId)) {
                shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.put(shardId, processor);
            } else {
                shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor.remove(shardId);
            }
        }
        return shardIdsAndStreamLetsOfShardsWithOnlyOneProcessor;
    }

    //@formatter:off (gets the formatting wrong)
    private void verifyAllRecordsWereConsumedExactlyOnce(List<Record> expectedRecords,
            List<Record> actualRecords) {
        //@formatter:on
        Assert.assertEquals(expectedRecords.size(), actualRecords.size());
        ListIterator<Record> expectedIter = expectedRecords.listIterator();
        ListIterator<Record> actualIter = actualRecords.listIterator();
        for (int i = 0; i < expectedRecords.size(); ++i) {
            Assert.assertEquals(expectedIter.next(), actualIter.next());
        }
    }

    //@formatter:off (gets the formatting wrong)
    private void verifyAllRecordsWereConsumedAtLeastOnce(List<Record> expectedRecords,
            List<Record> actualRecords) {
        //@formatter:on
        ListIterator<Record> expectedIter = expectedRecords.listIterator();
        for (int i = 0; i < expectedRecords.size(); ++i) {
            Record expectedRecord = expectedIter.next();
            Assert.assertTrue(actualRecords.contains(expectedRecord));
        }
    }

    private static class WorkerThread extends Thread {
        private final Worker worker;

        private WorkerThread(Worker worker) {
            super(worker);
            this.worker = worker;
        }

        public Worker getWorker() {
            return worker;
        }
    }*/
}
