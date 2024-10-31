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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.kinesis.checkpoint.Checkpoint;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.checkpoint.CheckpointFactory;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.exceptions.KinesisClientLibException;
import software.amazon.kinesis.exceptions.KinesisClientLibNonRetryableException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerMetricsTableConfig;
import software.amazon.kinesis.leases.LeaseManagementConfig.WorkerUtilizationAwareAssignmentConfig;
import software.amazon.kinesis.leases.LeaseManagementFactory;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseRefresher;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy.AutoDetectionAndDeferredDeletionStrategy;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy.ProvidedStreamsDeferredDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.atMost;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SchedulerTest {
    private final String tableName = "tableName";
    private final String workerIdentifier = "workerIdentifier";
    private final String applicationName = "applicationName";
    private final String streamName = "streamName";
    private final String namespace = "testNamespace";
    private static final long MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS = 1000L;
    private static final long MAX_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS = 30 * 1000L;
    private static final long LEASE_TABLE_CHECK_FREQUENCY_MILLIS = 3 * 1000L;
    private static final Region TEST_REGION = Region.US_EAST_2;
    private static final int ACCOUNT_ID_LENGTH = 12;
    private static final long TEST_ACCOUNT = Long.parseLong(StringUtils.repeat("1", ACCOUNT_ID_LENGTH));
    private static final long TEST_EPOCH = 1234567890L;
    private static final String TEST_SHARD_ID = "shardId-000000000001";
    private static final InitialPositionInStreamExtended TEST_INITIAL_POSITION =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    private Scheduler scheduler;
    private ShardRecordProcessorFactory shardRecordProcessorFactory;
    private CheckpointConfig checkpointConfig;
    private CoordinatorConfig coordinatorConfig;
    private LeaseManagementConfig leaseManagementConfig;
    private LifecycleConfig lifecycleConfig;
    private MetricsConfig metricsConfig;
    private ProcessorConfig processorConfig;
    private RetrievalConfig retrievalConfig;

    @Mock
    private KinesisAsyncClient kinesisClient;

    private final AmazonDynamoDBLocal embedded = DynamoDBEmbedded.create();
    private DynamoDbAsyncClient dynamoDBClient = embedded.dynamoDbAsyncClient();

    @Mock
    private CloudWatchAsyncClient cloudWatchClient;

    @Mock
    private RetrievalFactory retrievalFactory;

    @Mock
    private RecordsPublisher recordsPublisher;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    @Mock
    private ShardSyncTaskManager shardSyncTaskManager;

    @Mock
    private DynamoDBLeaseRefresher dynamoDBLeaseRefresher;

    @Mock
    private ShardDetector shardDetector;

    @Mock
    private Checkpointer checkpoint;

    @Mock
    private WorkerStateChangeListener workerStateChangeListener;

    @Spy
    private TestMultiStreamTracker multiStreamTracker;

    @Mock
    private LeaseCleanupManager leaseCleanupManager;

    private Map<StreamIdentifier, ShardSyncTaskManager> shardSyncTaskManagerMap;
    private Map<StreamIdentifier, ShardDetector> shardDetectorMap;

    @Before
    public void setup() {
        shardSyncTaskManagerMap = new HashMap<>();
        shardDetectorMap = new HashMap<>();
        shardRecordProcessorFactory = new TestShardRecordProcessorFactory();

        checkpointConfig = new CheckpointConfig().checkpointFactory(new TestKinesisCheckpointFactory());
        coordinatorConfig = new CoordinatorConfig(applicationName)
                .parentShardPollIntervalMillis(100L)
                .workerStateChangeListener(workerStateChangeListener);
        leaseManagementConfig = new LeaseManagementConfig(
                        tableName, applicationName, dynamoDBClient, kinesisClient, workerIdentifier)
                .leaseManagementFactory(new TestKinesisLeaseManagementFactory(false, false))
                .workerUtilizationAwareAssignmentConfig(new WorkerUtilizationAwareAssignmentConfig()
                        .disableWorkerMetrics(true)
                        .workerMetricsTableConfig(new WorkerMetricsTableConfig(applicationName)));
        lifecycleConfig = new LifecycleConfig();
        metricsConfig = new MetricsConfig(cloudWatchClient, namespace);
        processorConfig = new ProcessorConfig(shardRecordProcessorFactory);
        retrievalConfig =
                new RetrievalConfig(kinesisClient, streamName, applicationName).retrievalFactory(retrievalFactory);
        when(leaseCoordinator.leaseRefresher()).thenReturn(dynamoDBLeaseRefresher);
        when(leaseCoordinator.workerIdentifier()).thenReturn(workerIdentifier);
        when(dynamoDBLeaseRefresher.waitUntilLeaseOwnerToLeaseKeyIndexExists(anyLong(), anyLong()))
                .thenReturn(true);
        when(retrievalFactory.createGetRecordsCache(
                        any(ShardInfo.class), any(StreamConfig.class), any(MetricsFactory.class)))
                .thenReturn(recordsPublisher);
        when(kinesisClient.serviceClientConfiguration())
                .thenReturn(KinesisServiceClientConfiguration.builder()
                        .region(TEST_REGION)
                        .build());

        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
    }

    /**
     * Test method for {@link Scheduler#applicationName()}.
     */
    @Test
    public void testGetStageName() {
        final String stageName = "testStageName";
        coordinatorConfig = new CoordinatorConfig(stageName);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        assertEquals(stageName, scheduler.applicationName());
    }

    @Test
    public final void testCreateOrGetShardConsumer() {
        final String shardId = "shardId-000000000000";
        final String concurrencyToken = "concurrencyToken";
        final ShardInfo shardInfo = new ShardInfo(shardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardConsumer shardConsumer1 =
                scheduler.createOrGetShardConsumer(shardInfo, shardRecordProcessorFactory, leaseCleanupManager);
        assertNotNull(shardConsumer1);
        final ShardConsumer shardConsumer2 =
                scheduler.createOrGetShardConsumer(shardInfo, shardRecordProcessorFactory, leaseCleanupManager);
        assertNotNull(shardConsumer2);

        assertSame(shardConsumer1, shardConsumer2);

        final String anotherConcurrencyToken = "anotherConcurrencyToken";
        final ShardInfo shardInfo2 =
                new ShardInfo(shardId, anotherConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardConsumer shardConsumer3 =
                scheduler.createOrGetShardConsumer(shardInfo2, shardRecordProcessorFactory, leaseCleanupManager);
        assertNotNull(shardConsumer3);

        assertNotSame(shardConsumer1, shardConsumer3);
    }

    // TODO: figure out the behavior of the test.
    @Test
    public void testWorkerLoopWithCheckpoint() throws Exception {
        final String shardId = "shardId-000000000000";
        final String concurrencyToken = "concurrencyToken";
        final ExtendedSequenceNumber firstSequenceNumber = ExtendedSequenceNumber.TRIM_HORIZON;
        final ExtendedSequenceNumber secondSequenceNumber = new ExtendedSequenceNumber("1000");
        final ExtendedSequenceNumber finalSequenceNumber = new ExtendedSequenceNumber("2000");

        final List<ShardInfo> initialShardInfo =
                Collections.singletonList(new ShardInfo(shardId, concurrencyToken, null, firstSequenceNumber));
        final List<ShardInfo> firstShardInfo =
                Collections.singletonList(new ShardInfo(shardId, concurrencyToken, null, secondSequenceNumber));
        final List<ShardInfo> secondShardInfo =
                Collections.singletonList(new ShardInfo(shardId, concurrencyToken, null, finalSequenceNumber));

        final Checkpoint firstCheckpoint = new Checkpoint(firstSequenceNumber, null, null);

        when(leaseCoordinator.getCurrentAssignments()).thenReturn(initialShardInfo, firstShardInfo, secondShardInfo);
        when(checkpoint.getCheckpointObject(eq(shardId))).thenReturn(firstCheckpoint);

        Scheduler schedulerSpy = spy(scheduler);
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();

        verify(schedulerSpy)
                .buildConsumer(same(initialShardInfo.get(0)), eq(shardRecordProcessorFactory), eq(leaseCleanupManager));
        verify(schedulerSpy, never())
                .buildConsumer(same(firstShardInfo.get(0)), eq(shardRecordProcessorFactory), eq(leaseCleanupManager));
        verify(schedulerSpy, never())
                .buildConsumer(same(secondShardInfo.get(0)), eq(shardRecordProcessorFactory), eq(leaseCleanupManager));
        verify(checkpoint).getCheckpointObject(eq(shardId));
    }

    @Test
    public final void testCleanupShardConsumers() {
        final String shard0 = "shardId-000000000000";
        final String shard1 = "shardId-000000000001";
        final String concurrencyToken = "concurrencyToken";
        final String anotherConcurrencyToken = "anotherConcurrencyToken";

        final ShardInfo shardInfo0 = new ShardInfo(shard0, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardInfo shardInfo0WithAnotherConcurrencyToken =
                new ShardInfo(shard0, anotherConcurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        final ShardInfo shardInfo1 = new ShardInfo(shard1, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);

        final ShardConsumer shardConsumer0 =
                scheduler.createOrGetShardConsumer(shardInfo0, shardRecordProcessorFactory, leaseCleanupManager);
        final ShardConsumer shardConsumer0WithAnotherConcurrencyToken = scheduler.createOrGetShardConsumer(
                shardInfo0WithAnotherConcurrencyToken, shardRecordProcessorFactory, leaseCleanupManager);
        final ShardConsumer shardConsumer1 =
                scheduler.createOrGetShardConsumer(shardInfo1, shardRecordProcessorFactory, leaseCleanupManager);

        Set<ShardInfo> shards = new HashSet<>();
        shards.add(shardInfo0);
        shards.add(shardInfo1);
        scheduler.cleanupShardConsumers(shards);

        // verify shard consumer not present in assignedShards is shut down
        assertTrue(shardConsumer0WithAnotherConcurrencyToken.isShutdownRequested());
        // verify shard consumers present in assignedShards aren't shut down
        assertFalse(shardConsumer0.isShutdownRequested());
        assertFalse(shardConsumer1.isShutdownRequested());
    }

    @Test
    public final void testInitializationFailureWithRetries() throws Exception {
        doNothing().when(leaseCoordinator).initialize();
        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenThrow(new RuntimeException());
        leaseManagementConfig = new LeaseManagementConfig(
                        tableName, applicationName, dynamoDBClient, kinesisClient, workerIdentifier)
                .leaseManagementFactory(new TestKinesisLeaseManagementFactory(false, true));
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        scheduler.run();

        verify(dynamoDBLeaseRefresher, times(coordinatorConfig.maxInitializationAttempts()))
                .isLeaseTableEmpty();
    }

    @Test
    public final void testInitializationFailureWithRetriesWithConfiguredMaxInitializationAttempts() throws Exception {
        final int maxInitializationAttempts = 5;
        coordinatorConfig.maxInitializationAttempts(maxInitializationAttempts);
        leaseManagementConfig = new LeaseManagementConfig(
                        tableName, applicationName, dynamoDBClient, kinesisClient, workerIdentifier)
                .leaseManagementFactory(new TestKinesisLeaseManagementFactory(false, true));
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);

        doNothing().when(leaseCoordinator).initialize();
        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenThrow(new RuntimeException());

        scheduler.run();

        // verify initialization was retried for maxInitializationAttempts times
        verify(dynamoDBLeaseRefresher, times(maxInitializationAttempts)).isLeaseTableEmpty();
    }

    @Test
    public final void testMultiStreamInitialization() {
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        leaseManagementConfig = new LeaseManagementConfig(
                        tableName, applicationName, dynamoDBClient, kinesisClient, workerIdentifier)
                .leaseManagementFactory(new TestKinesisLeaseManagementFactory(true, true));
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        scheduler.initialize();
        shardDetectorMap.values().forEach(shardDetector -> verify(shardDetector, times(1))
                .listShards());
        shardSyncTaskManagerMap.values().forEach(shardSyncTM -> verify(shardSyncTM, times(1))
                .hierarchicalShardSyncer());
    }

    @Test
    public final void testMultiStreamInitializationWithFailures() {
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        leaseManagementConfig = new LeaseManagementConfig(
                        tableName, applicationName, dynamoDBClient, kinesisClient, workerIdentifier)
                .leaseManagementFactory(new TestKinesisLeaseManagementFactory(true, true));
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        scheduler.initialize();
        // Note : As of today we retry for all streams in the next attempt. Hence the retry for each stream will vary.
        //        At the least we expect 2 retries for each stream. Since there are 4 streams, we expect at most
        //        the number of calls to be 5.
        shardDetectorMap.values().forEach(shardDetector -> {
            verify(shardDetector, atLeast(2)).listShards();
            verify(shardDetector, atMost(5)).listShards();
        });
        shardSyncTaskManagerMap.values().forEach(shardSyncTM -> {
            verify(shardSyncTM, atLeast(2)).hierarchicalShardSyncer();
            verify(shardSyncTM, atMost(5)).hierarchicalShardSyncer();
        });
    }

    @Test
    public final void testMultiStreamConsumersAreBuiltOncePerAccountStreamShard() throws KinesisClientLibException {
        final String shardId = "shardId-000000000000";
        final String concurrencyToken = "concurrencyToken";
        final ExtendedSequenceNumber firstSequenceNumber = ExtendedSequenceNumber.TRIM_HORIZON;
        final ExtendedSequenceNumber secondSequenceNumber = new ExtendedSequenceNumber("1000");
        final ExtendedSequenceNumber finalSequenceNumber = new ExtendedSequenceNumber("2000");

        final List<ShardInfo> initialShardInfo = multiStreamTracker.streamConfigList().stream()
                .map(sc -> new ShardInfo(
                        shardId,
                        concurrencyToken,
                        null,
                        firstSequenceNumber,
                        sc.streamIdentifier().serialize()))
                .collect(Collectors.toList());
        final List<ShardInfo> firstShardInfo = multiStreamTracker.streamConfigList().stream()
                .map(sc -> new ShardInfo(
                        shardId,
                        concurrencyToken,
                        null,
                        secondSequenceNumber,
                        sc.streamIdentifier().serialize()))
                .collect(Collectors.toList());
        final List<ShardInfo> secondShardInfo = multiStreamTracker.streamConfigList().stream()
                .map(sc -> new ShardInfo(
                        shardId,
                        concurrencyToken,
                        null,
                        finalSequenceNumber,
                        sc.streamIdentifier().serialize()))
                .collect(Collectors.toList());

        final Checkpoint firstCheckpoint = new Checkpoint(firstSequenceNumber, null, null);

        when(leaseCoordinator.getCurrentAssignments()).thenReturn(initialShardInfo, firstShardInfo, secondShardInfo);
        when(checkpoint.getCheckpointObject(anyString())).thenReturn(firstCheckpoint);
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        Scheduler schedulerSpy = spy(scheduler);
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();

        initialShardInfo.forEach(shardInfo -> verify(schedulerSpy)
                .buildConsumer(same(shardInfo), eq(shardRecordProcessorFactory), same(leaseCleanupManager)));
        firstShardInfo.forEach(shardInfo -> verify(schedulerSpy, never())
                .buildConsumer(same(shardInfo), eq(shardRecordProcessorFactory), eq(leaseCleanupManager)));
        secondShardInfo.forEach(shardInfo -> verify(schedulerSpy, never())
                .buildConsumer(same(shardInfo), eq(shardRecordProcessorFactory), eq(leaseCleanupManager)));
    }

    @Test
    public final void testMultiStreamNoStreamsAreSyncedWhenStreamsAreNotRefreshed()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<StreamConfig> streamConfigList1 = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        List<StreamConfig> streamConfigList2 = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList1, streamConfigList2);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();
        Assert.assertTrue("SyncedStreams should be empty", syncedStreams.isEmpty());
        assertEquals(
                new HashSet<>(streamConfigList1),
                new HashSet<>(scheduler.currentStreamConfigMap().values()));
    }

    @Test
    public final void testMultiStreamOnlyNewStreamsAreSynced()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<StreamConfig> streamConfigList1 = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        List<StreamConfig> streamConfigList2 = IntStream.range(1, 7)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList1, streamConfigList2);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();
        Set<StreamIdentifier> expectedSyncedStreams = IntStream.range(5, 7)
                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(
                        Joiner.on(":").join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                .collect(Collectors.toCollection(HashSet::new));
        Assert.assertEquals(expectedSyncedStreams, syncedStreams);
        Assert.assertEquals(
                Sets.newHashSet(streamConfigList2),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
    }

    @Test
    public final void testMultiStreamSyncFromTableDefaultInitPos() {
        // Streams in lease table but not tracked by multiStreamTracker
        List<MultiStreamLease> leasesInTable = IntStream.range(1, 3)
                .mapToObj(streamId -> new MultiStreamLease()
                        .streamIdentifier(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345))
                        .shardId("some_random_shard_id"))
                .collect(Collectors.toCollection(LinkedList::new));
        // Include a stream that is already tracked by multiStreamTracker, just to make sure we will not touch this
        // stream config later
        leasesInTable.add(new MultiStreamLease()
                .streamIdentifier("123456789012:stream1:1")
                .shardId("some_random_shard_id"));

        // Expected StreamConfig after running syncStreamsFromLeaseTableOnAppInit
        // By default, Stream not present in multiStreamTracker will have initial position of LATEST
        List<StreamConfig> expectedConfig = IntStream.range(1, 3)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        // Include default configs
        expectedConfig.addAll(multiStreamTracker.streamConfigList());

        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        scheduler.syncStreamsFromLeaseTableOnAppInit(leasesInTable);
        Map<StreamIdentifier, StreamConfig> expectedConfigMap =
                expectedConfig.stream().collect(Collectors.toMap(StreamConfig::streamIdentifier, Function.identity()));
        Assert.assertEquals(expectedConfigMap, scheduler.currentStreamConfigMap());
    }

    @Test
    public final void testMultiStreamSyncFromTableCustomInitPos() {
        Date testTimeStamp = new Date();

        // Streams in lease table but not tracked by multiStreamTracker
        List<MultiStreamLease> leasesInTable = IntStream.range(1, 3)
                .mapToObj(streamId -> new MultiStreamLease()
                        .streamIdentifier(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345))
                        .shardId("some_random_shard_id"))
                .collect(Collectors.toCollection(LinkedList::new));
        // Include a stream that is already tracked by multiStreamTracker, just to make sure we will not touch this
        // stream config later
        leasesInTable.add(new MultiStreamLease()
                .streamIdentifier("123456789012:stream1:1")
                .shardId("some_random_shard_id"));

        // Expected StreamConfig after running syncStreamsFromLeaseTableOnAppInit
        // Stream not present in multiStreamTracker will have initial position specified by
        // orphanedStreamInitialPositionInStream
        List<StreamConfig> expectedConfig = IntStream.range(1, 3)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPositionAtTimestamp(testTimeStamp)))
                .collect(Collectors.toCollection(LinkedList::new));
        // Include default configs
        expectedConfig.addAll(multiStreamTracker.streamConfigList());

        // Mock a specific orphanedStreamInitialPositionInStream specified in multiStreamTracker
        when(multiStreamTracker.orphanedStreamInitialPositionInStream())
                .thenReturn(InitialPositionInStreamExtended.newInitialPositionAtTimestamp(testTimeStamp));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
        scheduler.syncStreamsFromLeaseTableOnAppInit(leasesInTable);
        Map<StreamIdentifier, StreamConfig> expectedConfigMap =
                expectedConfig.stream().collect(Collectors.toMap(sc -> sc.streamIdentifier(), sc -> sc));
        Assert.assertEquals(expectedConfigMap, scheduler.currentStreamConfigMap());
    }

    @Test
    public final void testMultiStreamStaleStreamsAreNotDeletedImmediatelyAutoDeletionStrategy()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new AutoDetectionAndDeferredDeletionStrategy() {
                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofHours(1);
                    }
                });
        testMultiStreamStaleStreamsAreNotDeletedImmediately(true, false);
    }

    @Test
    public final void testMultiStreamStaleStreamsAreNotDeletedImmediatelyNoDeletionStrategy()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy()).thenReturn(new NoLeaseDeletionStrategy());
        testMultiStreamStaleStreamsAreNotDeletedImmediately(false, true);
    }

    @Test
    public final void testMultiStreamStaleStreamsAreNotDeletedImmediatelyProvidedListStrategy()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new ProvidedStreamsDeferredDeletionStrategy() {
                    @Override
                    public List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
                        return null;
                    }

                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofHours(1);
                    }
                });
        testMultiStreamStaleStreamsAreNotDeletedImmediately(false, false);
    }

    @Test
    public final void testMultiStreamStaleStreamsAreNotDeletedImmediatelyProvidedListStrategy2()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new ProvidedStreamsDeferredDeletionStrategy() {
                    @Override
                    public List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
                        return IntStream.range(1, 3)
                                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                        .join(
                                                streamId * TEST_ACCOUNT,
                                                "multiStreamTest-" + streamId,
                                                streamId * 12345)))
                                .collect(Collectors.toCollection(ArrayList::new));
                    }

                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofHours(1);
                    }
                });
        testMultiStreamStaleStreamsAreNotDeletedImmediately(true, false);
    }

    private void testMultiStreamStaleStreamsAreNotDeletedImmediately(
            boolean expectPendingStreamsForDeletion, boolean onlyStreamsDeletionNotLeases)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<StreamConfig> streamConfigList1 = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        List<StreamConfig> streamConfigList2 = IntStream.range(3, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList1, streamConfigList2);
        mockListLeases(streamConfigList1);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();
        Set<StreamIdentifier> expectedPendingStreams = IntStream.range(1, 3)
                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(
                        Joiner.on(":").join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                .collect(Collectors.toCollection(HashSet::new));
        Set<StreamIdentifier> expectedSyncedStreams =
                onlyStreamsDeletionNotLeases ? expectedPendingStreams : Sets.newHashSet();
        Assert.assertEquals(expectedSyncedStreams, syncedStreams);
        Assert.assertEquals(
                Sets.newHashSet(onlyStreamsDeletionNotLeases ? streamConfigList2 : streamConfigList1),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        Assert.assertEquals(
                expectPendingStreamsForDeletion ? expectedPendingStreams : Sets.newHashSet(),
                scheduler.staleStreamDeletionMap().keySet());
    }

    @Test
    public final void testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriodWithAutoDetectionStrategy()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new AutoDetectionAndDeferredDeletionStrategy() {
                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ZERO;
                    }
                });
        testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriod(true, null);
    }

    @Test
    public final void testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriodWithProvidedListStrategy()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new ProvidedStreamsDeferredDeletionStrategy() {
                    @Override
                    public List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
                        return null;
                    }

                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ZERO;
                    }
                });
        HashSet<StreamConfig> currentStreamConfigMapOverride = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(HashSet::new));
        testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriod(false, currentStreamConfigMapOverride);
    }

    @Test
    public final void testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriodWithProvidedListStrategy2()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new ProvidedStreamsDeferredDeletionStrategy() {
                    @Override
                    public List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
                        return IntStream.range(1, 3)
                                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                        .join(
                                                streamId * TEST_ACCOUNT,
                                                "multiStreamTest-" + streamId,
                                                streamId * 12345)))
                                .collect(Collectors.toCollection(ArrayList::new));
                    }

                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ZERO;
                    }
                });
        testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriod(true, null);
    }

    private void testMultiStreamStaleStreamsAreDeletedAfterDefermentPeriod(
            boolean expectSyncedStreams, Set<StreamConfig> currentStreamConfigMapOverride)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<StreamConfig> streamConfigList1 = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        List<StreamConfig> streamConfigList2 = IntStream.range(3, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList1, streamConfigList2);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
        mockListLeases(streamConfigList1);

        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();
        Set<StreamIdentifier> expectedSyncedStreams = IntStream.range(1, 3)
                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(
                        Joiner.on(":").join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                .collect(Collectors.toCollection(HashSet::new));
        Assert.assertEquals(expectSyncedStreams ? expectedSyncedStreams : Sets.newHashSet(), syncedStreams);
        Assert.assertEquals(
                currentStreamConfigMapOverride == null
                        ? Sets.newHashSet(streamConfigList2)
                        : currentStreamConfigMapOverride,
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        Assert.assertEquals(
                Sets.newHashSet(), scheduler.staleStreamDeletionMap().keySet());
    }

    @Test
    public final void
            testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediatelyWithAutoDetectionStrategy()
                    throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new AutoDetectionAndDeferredDeletionStrategy() {
                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofHours(1);
                    }
                });
        testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediately(true, false);
    }

    @Test
    public final void testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediatelyWithNoDeletionStrategy()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy()).thenReturn(new NoLeaseDeletionStrategy());
        testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediately(false, true);
    }

    @Test
    public final void
            testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediatelyWithProvidedListStrategy()
                    throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new ProvidedStreamsDeferredDeletionStrategy() {
                    @Override
                    public List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
                        return null;
                    }

                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofHours(1);
                    }
                });
        testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediately(false, false);
    }

    @Test
    public final void
            testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediatelyWithProvidedListStrategy2()
                    throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new ProvidedStreamsDeferredDeletionStrategy() {
                    @Override
                    public List<StreamIdentifier> streamIdentifiersForLeaseCleanup() {
                        return IntStream.range(1, 3)
                                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                        .join(
                                                streamId * TEST_ACCOUNT,
                                                "multiStreamTest-" + streamId,
                                                streamId * 12345)))
                                .collect(Collectors.toCollection(ArrayList::new));
                    }

                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofHours(1);
                    }
                });
        testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediately(true, false);
    }

    private void testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreNotDeletedImmediately(
            boolean expectPendingStreamsForDeletion, boolean onlyStreamsNoLeasesDeletion)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<StreamConfig> streamConfigList1 = createDummyStreamConfigList(1, 5);
        List<StreamConfig> streamConfigList2 = createDummyStreamConfigList(3, 7);
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList1, streamConfigList2);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
        // Mock listLeases to exercise the delete path so scheduler doesn't remove stale streams due to not presenting
        // in lease table
        mockListLeases(streamConfigList1);
        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();
        Set<StreamIdentifier> expectedSyncedStreams;
        Set<StreamIdentifier> expectedPendingStreams = IntStream.range(1, 3)
                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(
                        Joiner.on(":").join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                .collect(Collectors.toCollection(HashSet::new));

        if (onlyStreamsNoLeasesDeletion) {
            expectedSyncedStreams = IntStream.concat(IntStream.range(1, 3), IntStream.range(5, 7))
                    .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(Joiner.on(":")
                            .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                    .collect(Collectors.toCollection(HashSet::new));
        } else {
            expectedSyncedStreams = IntStream.range(5, 7)
                    .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(Joiner.on(":")
                            .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                    .collect(Collectors.toCollection(HashSet::new));
        }

        Assert.assertEquals(expectedSyncedStreams, syncedStreams);
        List<StreamConfig> expectedCurrentStreamConfigs;
        if (onlyStreamsNoLeasesDeletion) {
            expectedCurrentStreamConfigs = IntStream.range(3, 7)
                    .mapToObj(streamId -> new StreamConfig(
                            StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                    .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                    .collect(Collectors.toCollection(LinkedList::new));
        } else {
            expectedCurrentStreamConfigs = IntStream.range(1, 7)
                    .mapToObj(streamId -> new StreamConfig(
                            StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                    .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                    .collect(Collectors.toCollection(LinkedList::new));
        }
        Assert.assertEquals(
                Sets.newHashSet(expectedCurrentStreamConfigs),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        Assert.assertEquals(
                expectPendingStreamsForDeletion ? expectedPendingStreams : Sets.newHashSet(),
                scheduler.staleStreamDeletionMap().keySet());
    }

    @Test
    public void testKinesisStaleDeletedStreamCleanup()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        List<StreamConfig> streamConfigList1 = createDummyStreamConfigList(1, 6);
        List<StreamConfig> streamConfigList2 = createDummyStreamConfigList(1, 4);

        prepareForStaleDeletedStreamCleanupTests(streamConfigList1, streamConfigList2);

        // when KCL starts it starts with tracking 5 stream
        assertEquals(
                Sets.newHashSet(streamConfigList1),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        assertEquals(0, scheduler.staleStreamDeletionMap().size());
        mockListLeases(streamConfigList1);

        // 2 Streams are no longer needed to be consumed
        Set<StreamIdentifier> syncedStreams1 = scheduler.checkAndSyncStreamShardsAndLeases();
        assertEquals(
                Sets.newHashSet(streamConfigList1),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        assertEquals(
                createDummyStreamConfigList(4, 6).stream()
                        .map(StreamConfig::streamIdentifier)
                        .collect(Collectors.toSet()),
                scheduler.staleStreamDeletionMap().keySet());
        assertEquals(0, syncedStreams1.size());

        StreamConfig deletedStreamConfig = createDummyStreamConfig(5);
        // One stream is deleted from Kinesis side
        scheduler.deletedStreamListProvider().add(deletedStreamConfig.streamIdentifier());

        Set<StreamIdentifier> syncedStreams2 = scheduler.checkAndSyncStreamShardsAndLeases();

        Set<StreamConfig> expectedCurrentStreamConfigs = Sets.newHashSet(streamConfigList1);
        expectedCurrentStreamConfigs.remove(deletedStreamConfig);

        // assert kinesis deleted stream is cleaned up from KCL in memory state.
        assertEquals(
                expectedCurrentStreamConfigs,
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        assertEquals(
                Sets.newHashSet(createDummyStreamConfig(4).streamIdentifier()),
                Sets.newHashSet(scheduler.staleStreamDeletionMap().keySet()));
        assertEquals(1, syncedStreams2.size());
        assertEquals(
                0, scheduler.deletedStreamListProvider().purgeAllDeletedStream().size());

        verify(multiStreamTracker, times(3)).streamConfigList();
    }

    // Tests validate that no cleanup of stream is done if its still tracked in multiStreamTracker
    @Test
    public void testKinesisStaleDeletedStreamNoCleanUpForTrackedStream()
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        List<StreamConfig> streamConfigList1 = createDummyStreamConfigList(1, 6);
        prepareForStaleDeletedStreamCleanupTests(streamConfigList1);

        scheduler.deletedStreamListProvider().add(createDummyStreamConfig(3).streamIdentifier());

        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();

        assertEquals(0, syncedStreams.size());
        assertEquals(0, scheduler.staleStreamDeletionMap().size());
        assertEquals(
                Sets.newHashSet(streamConfigList1),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
    }

    // Creates list of upperBound-lowerBound no of dummy StreamConfig
    private List<StreamConfig> createDummyStreamConfigList(int lowerBound, int upperBound) {
        return IntStream.range(lowerBound, upperBound)
                .mapToObj(this::createDummyStreamConfig)
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private StreamConfig createDummyStreamConfig(int streamId) {
        return new StreamConfig(
                StreamIdentifier.multiStreamInstance(
                        Joiner.on(":").join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));
    }

    @Test
    public final void testMultiStreamNewStreamsAreSyncedAndStaleStreamsAreDeletedAfterDefermentPeriod()
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        List<StreamConfig> streamConfigList1 = IntStream.range(1, 5)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        List<StreamConfig> streamConfigList2 = IntStream.range(3, 7)
                .mapToObj(streamId -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(Joiner.on(":")
                                .join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)))
                .collect(Collectors.toCollection(LinkedList::new));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList1, streamConfigList2);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
        Set<StreamIdentifier> syncedStreams = scheduler.checkAndSyncStreamShardsAndLeases();
        Set<StreamIdentifier> expectedSyncedStreams = IntStream.concat(IntStream.range(1, 3), IntStream.range(5, 7))
                .mapToObj(streamId -> StreamIdentifier.multiStreamInstance(
                        Joiner.on(":").join(streamId * TEST_ACCOUNT, "multiStreamTest-" + streamId, streamId * 12345)))
                .collect(Collectors.toCollection(HashSet::new));
        Assert.assertEquals(expectedSyncedStreams, syncedStreams);
        Assert.assertEquals(
                Sets.newHashSet(streamConfigList2),
                Sets.newHashSet(scheduler.currentStreamConfigMap().values()));
        Assert.assertEquals(
                Sets.newHashSet(), scheduler.staleStreamDeletionMap().keySet());
    }

    @Test
    public final void testInitializationWaitsWhenLeaseTableIsEmpty() throws Exception {
        final int maxInitializationAttempts = 1;
        coordinatorConfig.maxInitializationAttempts(maxInitializationAttempts);
        coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist(false);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(true);

        long startTime = System.currentTimeMillis();
        scheduler.shouldInitiateLeaseSync();
        long endTime = System.currentTimeMillis();

        assertTrue(endTime - startTime > MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS);
        assertTrue(endTime - startTime
                < (MAX_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS + LEASE_TABLE_CHECK_FREQUENCY_MILLIS));
    }

    @Test
    public final void testInitializationDoesntWaitWhenLeaseTableIsNotEmpty() throws Exception {
        final int maxInitializationAttempts = 1;
        coordinatorConfig.maxInitializationAttempts(maxInitializationAttempts);
        coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist(false);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);

        when(dynamoDBLeaseRefresher.isLeaseTableEmpty()).thenReturn(false);

        long startTime = System.currentTimeMillis();
        scheduler.shouldInitiateLeaseSync();
        long endTime = System.currentTimeMillis();

        assertTrue(endTime - startTime < MIN_WAIT_TIME_FOR_LEASE_TABLE_CHECK_MILLIS);
    }

    @Test
    public final void testSchedulerShutdown() {
        scheduler.shutdown();
        verify(workerStateChangeListener, times(1))
                .onWorkerStateChange(WorkerStateChangeListener.WorkerState.SHUT_DOWN_STARTED);
        verify(leaseCoordinator, times(1)).stop();
        verify(workerStateChangeListener, times(1))
                .onWorkerStateChange(WorkerStateChangeListener.WorkerState.SHUT_DOWN);
    }

    @Test
    public void testErrorHandlerForUndeliverableAsyncTaskExceptions() {
        DiagnosticEventFactory eventFactory = mock(DiagnosticEventFactory.class);
        ExecutorStateEvent executorStateEvent = mock(ExecutorStateEvent.class);
        RejectedTaskEvent rejectedTaskEvent = mock(RejectedTaskEvent.class);

        when(eventFactory.rejectedTaskEvent(any(), any())).thenReturn(rejectedTaskEvent);
        when(eventFactory.executorStateEvent(any(), any())).thenReturn(executorStateEvent);

        Scheduler testScheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig,
                eventFactory);

        Scheduler schedulerSpy = spy(testScheduler);

        // reject task on third loop
        doCallRealMethod()
                .doCallRealMethod()
                .doAnswer(invocation -> {
                    // trigger rejected task in RxJava layer
                    RxJavaPlugins.onError(new RejectedExecutionException("Test exception."));
                    return null;
                })
                .when(schedulerSpy)
                .runProcessLoop();

        // Scheduler sets error handler in initialize method
        schedulerSpy.initialize();
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();
        schedulerSpy.runProcessLoop();

        verify(eventFactory, times(1)).rejectedTaskEvent(eq(executorStateEvent), any());
        verify(rejectedTaskEvent, times(1)).accept(any());
    }

    @Test
    public void testUpdateStreamMapIfMissingLatestStream() throws Exception {
        prepareMultiStreamScheduler(createDummyStreamConfigList(1, 6));
        scheduler.checkAndSyncStreamShardsAndLeases();
        verify(scheduler).syncStreamsFromLeaseTableOnAppInit(any());
    }

    @Test
    public void testSyncLeaseAsThisIsInitialAppBootstrapEvenThoughStreamMapContainsAllStreams() {
        final List<StreamConfig> streamConfigList = createDummyStreamConfigList(1, 6);
        when(multiStreamTracker.streamConfigList()).thenReturn(Collections.emptyList());
        prepareMultiStreamScheduler(streamConfigList);
        // Populate currentStreamConfigMap to simulate that the leader has the latest streams.
        multiStreamTracker
                .streamConfigList()
                .forEach(s -> scheduler.currentStreamConfigMap().put(s.streamIdentifier(), s));
        scheduler.initialize();
        scheduler.runProcessLoop();
        verify(scheduler).syncStreamsFromLeaseTableOnAppInit(any());
        assertTrue(scheduler.currentStreamConfigMap().size() != 0);
    }

    @Test
    public void testNotRefreshForNewStreamAfterLeaderFlippedTheShouldInitialize() {
        prepareMultiStreamScheduler(createDummyStreamConfigList(1, 6));
        scheduler.initialize();
        // flip the shouldInitialize flag
        scheduler.runProcessLoop();
        verify(scheduler, times(1)).syncStreamsFromLeaseTableOnAppInit(any());

        final List<StreamConfig> streamConfigList = createDummyStreamConfigList(1, 6);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList);
        scheduler.runProcessLoop();

        // Since the sync path has been executed once before the DDB sync flags should be flipped
        // to prevent doing DDB lookups in the subsequent runs.
        verify(scheduler, times(1)).syncStreamsFromLeaseTableOnAppInit(any());
        assertEquals(
                0,
                streamConfigList.stream()
                        .filter(s -> !scheduler.currentStreamConfigMap().containsKey(s.streamIdentifier()))
                        .count());
    }

    @Test
    public void testDropStreamsFromMapsWhenStreamIsNotInLeaseTableAndNewStreamConfigMap() throws Exception {
        when(multiStreamTracker.streamConfigList()).thenReturn(Collections.emptyList());
        prepareMultiStreamScheduler();
        final List<StreamConfig> streamConfigList = createDummyStreamConfigList(1, 6);
        streamConfigList.forEach(s -> scheduler.currentStreamConfigMap().put(s.streamIdentifier(), s));
        scheduler.checkAndSyncStreamShardsAndLeases();
        assertEquals(Collections.emptySet(), scheduler.currentStreamConfigMap().keySet());
    }

    @Test
    public void testNotDropStreamsFromMapsWhenStreamIsInLeaseTable() throws Exception {
        when(multiStreamTracker.streamConfigList()).thenReturn(Collections.emptyList());
        prepareForStaleDeletedStreamCleanupTests();
        final List<StreamConfig> streamConfigList = createDummyStreamConfigList(1, 6);
        mockListLeases(streamConfigList);
        streamConfigList.forEach(s -> scheduler.currentStreamConfigMap().put(s.streamIdentifier(), s));
        final Set<StreamIdentifier> initialSet =
                new HashSet<>(scheduler.currentStreamConfigMap().keySet());
        scheduler.checkAndSyncStreamShardsAndLeases();
        assertEquals(initialSet, scheduler.currentStreamConfigMap().keySet());
        assertEquals(
                streamConfigList.size(),
                scheduler.currentStreamConfigMap().keySet().size());
    }

    @Test
    public void testNotDropStreamsFromMapsWhenStreamIsInNewStreamConfigMap() throws Exception {
        final List<StreamConfig> streamConfigList = createDummyStreamConfigList(1, 6);
        when(multiStreamTracker.streamConfigList()).thenReturn(streamConfigList);
        prepareMultiStreamScheduler();
        streamConfigList.forEach(s -> scheduler.currentStreamConfigMap().put(s.streamIdentifier(), s));
        final Set<StreamIdentifier> initialSet =
                new HashSet<>(scheduler.currentStreamConfigMap().keySet());
        scheduler.checkAndSyncStreamShardsAndLeases();
        assertEquals(initialSet, scheduler.currentStreamConfigMap().keySet());
        assertEquals(
                streamConfigList.size(),
                scheduler.currentStreamConfigMap().keySet().size());
    }

    @SafeVarargs
    private final void prepareMultiStreamScheduler(List<StreamConfig>... streamConfigs) {
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        scheduler = spy(new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig));
        stubMultiStreamTracker(streamConfigs);
        when(scheduler.shouldSyncStreamsNow()).thenReturn(true);
    }

    @SafeVarargs
    private final void prepareForStaleDeletedStreamCleanupTests(List<StreamConfig>... streamConfigs) {
        when(multiStreamTracker.formerStreamsLeasesDeletionStrategy())
                .thenReturn(new AutoDetectionAndDeferredDeletionStrategy() {
                    @Override
                    public Duration waitPeriodToDeleteFormerStreams() {
                        return Duration.ofDays(1);
                    }
                });
        stubMultiStreamTracker(streamConfigs);
        prepareMultiStreamScheduler();
    }

    @SafeVarargs
    private final void stubMultiStreamTracker(List<StreamConfig>... streamConfigs) {
        if (streamConfigs.length > 0) {
            OngoingStubbing<List<StreamConfig>> stub = when(multiStreamTracker.streamConfigList());
            for (List<StreamConfig> streamConfig : streamConfigs) {
                stub = stub.thenReturn(streamConfig);
            }
        }
    }

    private void mockListLeases(List<StreamConfig> configs)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        when(dynamoDBLeaseRefresher.listLeases())
                .thenReturn(configs.stream()
                        .map(s -> new MultiStreamLease()
                                .streamIdentifier(s.streamIdentifier().toString())
                                .shardId("some_random_shard_id"))
                        .collect(Collectors.toList()));
    }

    @Test
    public void testStreamConfigsArePopulatedWithStreamArnsInMultiStreamMode() {
        final String streamArnStr = constructStreamArnStr(TEST_REGION, 111122223333L, "some-stream-name");
        when(multiStreamTracker.streamConfigList())
                .thenReturn(Stream.of(
                                // Each of scheduler's currentStreamConfigMap entries should have a streamARN in
                                // multi-stream mode, regardless of whether the streamTracker-provided streamIdentifiers
                                // were created using serialization or stream ARN.
                                StreamIdentifier.multiStreamInstance(
                                        constructStreamIdentifierSer(TEST_ACCOUNT, streamName)),
                                StreamIdentifier.multiStreamInstance(Arn.fromString(streamArnStr), TEST_EPOCH))
                        .map(streamIdentifier -> new StreamConfig(streamIdentifier, TEST_INITIAL_POSITION))
                        .collect(Collectors.toList()));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);

        final Set<String> expectedStreamArns =
                Sets.newHashSet(constructStreamArnStr(TEST_REGION, TEST_ACCOUNT, streamName), streamArnStr);

        final Set<String> actualStreamArns = scheduler.currentStreamConfigMap().values().stream()
                .map(sc -> sc.streamIdentifier()
                        .streamArnOptional()
                        .orElseThrow(IllegalStateException::new)
                        .toString())
                .collect(Collectors.toSet());

        assertEquals(expectedStreamArns, actualStreamArns);
    }

    @Test
    public void testOrphanStreamConfigIsPopulatedWithArn() {
        final String streamIdentifierSerializationForOrphan = constructStreamIdentifierSer(TEST_ACCOUNT, streamName);
        assertFalse(multiStreamTracker.streamConfigList().stream()
                .map(sc -> sc.streamIdentifier().serialize())
                .collect(Collectors.toSet())
                .contains(streamIdentifierSerializationForOrphan));

        when(leaseCoordinator.getCurrentAssignments())
                .thenReturn(Collections.singletonList(
                        new ShardInfo(TEST_SHARD_ID, null, null, null, streamIdentifierSerializationForOrphan)));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);
        scheduler = new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);

        scheduler.runProcessLoop();

        verify(multiStreamTracker)
                .createStreamConfig(StreamIdentifier.multiStreamInstance(streamIdentifierSerializationForOrphan));

        final ArgumentCaptor<StreamConfig> streamConfigArgumentCaptor = ArgumentCaptor.forClass(StreamConfig.class);
        verify(retrievalFactory).createGetRecordsCache(any(), streamConfigArgumentCaptor.capture(), any());

        final StreamConfig actualStreamConfigForOrphan = streamConfigArgumentCaptor.getValue();
        final Optional<Arn> streamArnForOrphan =
                actualStreamConfigForOrphan.streamIdentifier().streamArnOptional();
        assertTrue(streamArnForOrphan.isPresent());
        assertEquals(
                constructStreamArnStr(TEST_REGION, TEST_ACCOUNT, streamName),
                streamArnForOrphan.get().toString());
    }

    @Test
    public void testMismatchingArnRegionAndKinesisClientRegionThrowsException() {
        final Region streamArnRegion = Region.US_WEST_1;
        Assert.assertNotEquals(
                streamArnRegion, kinesisClient.serviceClientConfiguration().region());

        when(multiStreamTracker.streamConfigList())
                .thenReturn(Collections.singletonList(new StreamConfig(
                        StreamIdentifier.multiStreamInstance(
                                Arn.fromString(constructStreamArnStr(streamArnRegion, TEST_ACCOUNT, streamName)),
                                TEST_EPOCH),
                        TEST_INITIAL_POSITION)));
        retrievalConfig = new RetrievalConfig(kinesisClient, multiStreamTracker, applicationName)
                .retrievalFactory(retrievalFactory);

        assertThrows(
                IllegalArgumentException.class,
                () -> new Scheduler(
                        checkpointConfig,
                        coordinatorConfig,
                        leaseManagementConfig,
                        lifecycleConfig,
                        metricsConfig,
                        processorConfig,
                        retrievalConfig));
    }

    private static String constructStreamIdentifierSer(long accountId, String streamName) {
        return String.join(":", String.valueOf(accountId), streamName, String.valueOf(TEST_EPOCH));
    }

    private static String constructStreamArnStr(Region region, long accountId, String streamName) {
        return "arn:aws:kinesis:" + region + ":" + accountId + ":stream/" + streamName;
    }

    /*private void runAndTestWorker(int numShards, int threadPoolSize) throws Exception {
        final int numberOfRecordsPerShard = 10;
        final String kinesisShardPrefix = "kinesis-0-";
        final BigInteger startSeqNum = BigInteger.ONE;
        List<Shard> shardList = KinesisLocalFileDataCreator.createShardList(numShards, kinesisShardPrefix, startSeqNum);
        Assert.assertEquals(numShards, shardList.size());
        List<Lease> initialLeases = new ArrayList<Lease>();
        for (Shard shard : shardList) {
            Lease lease = ShardSyncer.newKCLLease(shard);
            lease.setCheckpoint(ExtendedSequenceNumber.AT_TIMESTAMP);
            initialLeases.add(lease);
        }
        runAndTestWorker(shardList, threadPoolSize, initialLeases, numberOfRecordsPerShard);
    }

    private void runAndTestWorker(List<Shard> shardList,
                                  int threadPoolSize,
                                  List<Lease> initialLeases,
                                  int numberOfRecordsPerShard) throws Exception {
        File file = KinesisLocalFileDataCreator.generateTempDataFile(shardList, numberOfRecordsPerShard, "unitTestWT001");
        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        Semaphore recordCounter = new Semaphore(0);
        ShardSequenceVerifier shardSequenceVerifier = new ShardSequenceVerifier(shardList);
        TestStreamletFactory recordProcessorFactory = new TestStreamletFactory(recordCounter, shardSequenceVerifier);

        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        SchedulerThread schedulerThread = runWorker(initialLeases);

        // TestStreamlet will release the semaphore once for every record it processes
        recordCounter.acquire(numberOfRecordsPerShard * shardList.size());

        // Wait a bit to allow the worker to spin against the end of the stream.
        Thread.sleep(500L);

        testWorker(shardList, threadPoolSize, initialLeases,
                numberOfRecordsPerShard, fileBasedProxy, recordProcessorFactory);

        schedulerThread.schedulerForThread().shutdown();
        executorService.shutdownNow();
        file.delete();
    }

    private SchedulerThread runWorker(final List<Lease> initialLeases) throws Exception {
        final int maxRecords = 2;

        final long leaseDurationMillis = 10000L;
        final long epsilonMillis = 1000L;
        final long idleTimeInMilliseconds = 2L;

        AmazonDynamoDB ddbClient = DynamoDBEmbedded.create().dynamoDBClient();
        LeaseManager<Lease> leaseRefresher = new LeaseManager("foo", ddbClient);
        leaseRefresher.createLeaseTableIfNotExists(1L, 1L);
        for (Lease initialLease : initialLeases) {
            leaseRefresher.createLeaseIfNotExists(initialLease);
        }

        checkpointConfig = new CheckpointConfig("foo", ddbClient, workerIdentifier)
                .failoverTimeMillis(leaseDurationMillis)
                .epsilonMillis(epsilonMillis)
                .leaseRefresher(leaseRefresher);
        leaseManagementConfig = new LeaseManagementConfig("foo", ddbClient, kinesisClient, streamName, workerIdentifier)
                .failoverTimeMillis(leaseDurationMillis)
                .epsilonMillis(epsilonMillis);
        retrievalConfig.initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
                        new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP)))
                .maxRecords(maxRecords)
                .idleTimeBetweenReadsInMillis(idleTimeInMilliseconds);
        scheduler = new Scheduler(checkpointConfig, coordinatorConfig, leaseManagementConfig, lifecycleConfig,
                metricsConfig, processorConfig, retrievalConfig);

        SchedulerThread schedulerThread = new SchedulerThread(scheduler);
        schedulerThread.start();
        return schedulerThread;
    }

    private void testWorker(List<Shard> shardList,
                            int threadPoolSize,
                            List<Lease> initialLeases,
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
        shardList.forEach(shard -> {
            final String iterator = kinesisProxy.getIterator(shard.shardId(), new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            final List<Record> records = kinesisProxy.get(iterator, numberOfRecordsPerShard).records();
            assertEquals();
        });
        for (Shard shard : shardList) {
            String shardId = shard.shardId();
            String iterator =
                    fileBasedProxy.getIterator(shardId, new Date(KinesisLocalFileDataCreator.STARTING_TIMESTAMP));
            List<Record> expectedRecords = fileBasedProxy.get(iterator, numRecs).records();
            verifyAllRecordsWereConsumedAtLeastOnce(expectedRecords, shardStreamletsRecords.get(shardId));
        }

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

    @Data
    @EqualsAndHashCode(callSuper = true)
    @Accessors(fluent = true)
    private static class SchedulerThread extends Thread {
        private final Scheduler schedulerForThread;
    }*/

    private static class TestShardRecordProcessorFactory implements ShardRecordProcessorFactory {
        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return new ShardRecordProcessor() {
                @Override
                public void initialize(final InitializationInput initializationInput) {
                    // Do nothing.
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
                public void leaseLost(LeaseLostInput leaseLostInput) {}

                @Override
                public void shardEnded(ShardEndedInput shardEndedInput) {
                    try {
                        shardEndedInput.checkpointer().checkpoint();
                    } catch (KinesisClientLibNonRetryableException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {}
            };
        }

        @Override
        public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
            return shardRecordProcessor();
        }
    }

    @RequiredArgsConstructor
    private class TestKinesisLeaseManagementFactory implements LeaseManagementFactory {

        private final boolean shardSyncFirstAttemptFailure;
        private final boolean shouldReturnDefaultShardSyncTaskmanager;

        @Override
        public LeaseCoordinator createLeaseCoordinator(MetricsFactory metricsFactory) {
            return leaseCoordinator;
        }

        @Override
        public LeaseCoordinator createLeaseCoordinator(
                MetricsFactory metricsFactory, ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap) {
            return leaseCoordinator;
        }

        @Override
        public ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory) {
            return shardSyncTaskManager;
        }

        @Override
        public ShardSyncTaskManager createShardSyncTaskManager(
                MetricsFactory metricsFactory,
                StreamConfig streamConfig,
                DeletedStreamListProvider deletedStreamListProvider) {
            if (shouldReturnDefaultShardSyncTaskmanager) {
                return shardSyncTaskManager;
            }
            final ShardSyncTaskManager shardSyncTaskManager = mock(ShardSyncTaskManager.class);
            final ShardDetector shardDetector = mock(ShardDetector.class);
            shardSyncTaskManagerMap.put(streamConfig.streamIdentifier(), shardSyncTaskManager);
            shardDetectorMap.put(streamConfig.streamIdentifier(), shardDetector);
            when(shardSyncTaskManager.shardDetector()).thenReturn(shardDetector);
            final HierarchicalShardSyncer hierarchicalShardSyncer = new HierarchicalShardSyncer();
            when(shardSyncTaskManager.hierarchicalShardSyncer()).thenReturn(hierarchicalShardSyncer);
            if (shardSyncFirstAttemptFailure) {
                when(shardDetector.listShards())
                        .thenThrow(new RuntimeException("Service Exception"))
                        .thenReturn(Collections.EMPTY_LIST);
            }
            return shardSyncTaskManager;
        }

        @Override
        public DynamoDBLeaseRefresher createLeaseRefresher() {
            return dynamoDBLeaseRefresher;
        }

        @Override
        public ShardDetector createShardDetector() {
            return shardDetector;
        }

        @Override
        public ShardDetector createShardDetector(StreamConfig streamConfig) {
            return shardDetectorMap.get(streamConfig.streamIdentifier());
        }

        @Override
        public LeaseCleanupManager createLeaseCleanupManager(MetricsFactory metricsFactory) {
            return leaseCleanupManager;
        }
    }

    private class TestKinesisCheckpointFactory implements CheckpointFactory {
        @Override
        public Checkpointer createCheckpointer(
                final LeaseCoordinator leaseCoordinator, final LeaseRefresher leaseRefresher) {
            return checkpoint;
        }
    }

    // TODO: Upgrade to mockito >= 2.7.13, and use Spy on MultiStreamTracker to directly access the default methods
    //  without implementing TestMultiStreamTracker class
    @NoArgsConstructor
    private class TestMultiStreamTracker implements MultiStreamTracker {
        @Override
        public List<StreamConfig> streamConfigList() {
            final InitialPositionInStreamExtended latest =
                    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

            return Arrays.asList(
                    new StreamConfig(StreamIdentifier.multiStreamInstance("123456789012:stream1:1"), latest),
                    new StreamConfig(StreamIdentifier.multiStreamInstance("123456789012:stream2:2"), latest),
                    new StreamConfig(StreamIdentifier.multiStreamInstance("210987654321:stream1:1"), latest),
                    new StreamConfig(StreamIdentifier.multiStreamInstance("210987654321:stream2:3"), latest));
        }

        @Override
        public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
            return new AutoDetectionAndDeferredDeletionStrategy() {
                @Override
                public Duration waitPeriodToDeleteFormerStreams() {
                    return Duration.ZERO;
                }
            };
        }
    }
}
