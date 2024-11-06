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
package software.amazon.kinesis.lifecycle;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.hamcrest.Condition;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.LeaseStatsRecorder;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.ShardObjectHelper;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.schemaregistry.SchemaRegistryDecoder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.lifecycle.ConsumerStates.ShardConsumerState;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerStatesTest {
    private static final String STREAM_NAME = "TestStream";
    private static final InitialPositionInStreamExtended INITIAL_POSITION_IN_STREAM =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    private ShardConsumer consumer;
    private ShardConsumerArgument argument;

    @Mock
    private ShardRecordProcessor shardRecordProcessor;

    @Mock
    private ShardRecordProcessorCheckpointer recordProcessorCheckpointer;

    @Mock
    private ExecutorService executorService;

    @Mock
    private ShardInfo shardInfo;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    @Mock
    private LeaseRefresher leaseRefresher;

    @Mock
    private LeaseStatsRecorder leaseStatsRecorder;

    @Mock
    private Checkpointer checkpointer;

    @Mock
    private ShutdownNotification shutdownNotification;

    @Mock
    private RecordsPublisher recordsPublisher;

    @Mock
    private ShardDetector shardDetector;

    @Mock
    private HierarchicalShardSyncer hierarchicalShardSyncer;

    @Mock
    private MetricsFactory metricsFactory;

    @Mock
    private ProcessRecordsInput processRecordsInput;

    @Mock
    private TaskExecutionListener taskExecutionListener;

    @Mock
    private LeaseCleanupManager leaseCleanupManager;

    private long parentShardPollIntervalMillis = 0xCAFE;
    private boolean cleanupLeasesOfCompletedShards = true;
    private long taskBackoffTimeMillis = 0xBEEF;
    private ShutdownReason reason = ShutdownReason.SHARD_END;
    private boolean skipShardSyncAtWorkerInitializationIfLeasesExist = true;
    private long listShardsBackoffTimeInMillis = 50L;
    private int maxListShardsRetryAttempts = 10;
    private boolean shouldCallProcessRecordsEvenForEmptyRecordList = true;
    private boolean ignoreUnexpectedChildShards = false;
    private long idleTimeInMillis = 1000L;
    private Optional<Long> logWarningForTaskAfterMillis = Optional.empty();
    private SchemaRegistryDecoder schemaRegistryDecoder = null;

    @Before
    public void setup() {
        argument = new ShardConsumerArgument(
                shardInfo,
                StreamIdentifier.singleStreamInstance(STREAM_NAME),
                leaseCoordinator,
                executorService,
                recordsPublisher,
                shardRecordProcessor,
                checkpointer,
                recordProcessorCheckpointer,
                parentShardPollIntervalMillis,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                listShardsBackoffTimeInMillis,
                maxListShardsRetryAttempts,
                shouldCallProcessRecordsEvenForEmptyRecordList,
                idleTimeInMillis,
                INITIAL_POSITION_IN_STREAM,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                shardDetector,
                new AggregatorUtil(),
                hierarchicalShardSyncer,
                metricsFactory,
                leaseCleanupManager,
                schemaRegistryDecoder);
        when(shardInfo.shardId()).thenReturn("shardId-000000000000");
        when(shardInfo.streamIdentifierSerOpt())
                .thenReturn(Optional.of(
                        StreamIdentifier.singleStreamInstance(STREAM_NAME).serialize()));
        consumer = spy(new ShardConsumer(
                recordsPublisher,
                executorService,
                shardInfo,
                logWarningForTaskAfterMillis,
                argument,
                taskExecutionListener,
                0));
        when(recordProcessorCheckpointer.checkpointer()).thenReturn(checkpointer);
    }

    private static final Class<LeaseRefresher> LEASE_REFRESHER_CLASS = LeaseRefresher.class;

    @Test
    public void blockOnParentStateTest() {
        ConsumerState state = ShardConsumerState.WAITING_ON_PARENT_SHARDS.consumerState();
        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);

        ConsumerTask task = state.createTask(argument, consumer, null);

        assertThat(task, taskWith(BlockOnParentShardTask.class, ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(
                task,
                taskWith(
                        BlockOnParentShardTask.class,
                        LEASE_REFRESHER_CLASS,
                        "leaseRefresher",
                        equalTo(leaseRefresher)));
        assertThat(
                task,
                taskWith(
                        BlockOnParentShardTask.class,
                        Long.class,
                        "parentShardPollIntervalMillis",
                        equalTo(parentShardPollIntervalMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.INITIALIZING.consumerState()));
        for (ShutdownReason shutdownReason : ShutdownReason.values()) {
            assertThat(
                    state.shutdownTransition(shutdownReason),
                    equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        }

        assertThat(state.state(), equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS));
        assertThat(state.taskType(), equalTo(TaskType.BLOCK_ON_PARENT_SHARDS));
    }

    @Test
    public void initializingStateTest() {
        ConsumerState state = ShardConsumerState.INITIALIZING.consumerState();
        ConsumerTask task = state.createTask(argument, consumer, null);

        assertThat(task, initTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, initTask(ShardRecordProcessor.class, "shardRecordProcessor", equalTo(shardRecordProcessor)));
        assertThat(task, initTask(Checkpointer.class, "checkpoint", equalTo(checkpointer)));
        assertThat(
                task,
                initTask(
                        ShardRecordProcessorCheckpointer.class,
                        "recordProcessorCheckpointer",
                        equalTo(recordProcessorCheckpointer)));
        assertThat(task, initTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.consumerState()));

        assertThat(
                state.shutdownTransition(ShutdownReason.LEASE_LOST),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.SHARD_END),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.consumerState()));

        assertThat(state.state(), equalTo(ShardConsumerState.INITIALIZING));
        assertThat(state.taskType(), equalTo(TaskType.INITIALIZE));
    }

    @Test
    public void processingStateTestSynchronous() {
        when(leaseCoordinator.leaseStatsRecorder()).thenReturn(leaseStatsRecorder);
        ConsumerState state = ShardConsumerState.PROCESSING.consumerState();
        ConsumerTask task = state.createTask(argument, consumer, null);

        assertThat(task, procTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, procTask(ShardRecordProcessor.class, "shardRecordProcessor", equalTo(shardRecordProcessor)));
        assertThat(
                task,
                procTask(
                        ShardRecordProcessorCheckpointer.class,
                        "recordProcessorCheckpointer",
                        equalTo(recordProcessorCheckpointer)));
        assertThat(task, procTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.consumerState()));

        assertThat(
                state.shutdownTransition(ShutdownReason.LEASE_LOST),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.SHARD_END),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.consumerState()));

        assertThat(state.state(), equalTo(ShardConsumerState.PROCESSING));
        assertThat(state.taskType(), equalTo(TaskType.PROCESS));
    }

    @Test
    public void processingStateTestAsynchronous() {
        when(leaseCoordinator.leaseStatsRecorder()).thenReturn(leaseStatsRecorder);
        ConsumerState state = ShardConsumerState.PROCESSING.consumerState();
        ConsumerTask task = state.createTask(argument, consumer, null);

        assertThat(task, procTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, procTask(ShardRecordProcessor.class, "shardRecordProcessor", equalTo(shardRecordProcessor)));
        assertThat(
                task,
                procTask(
                        ShardRecordProcessorCheckpointer.class,
                        "recordProcessorCheckpointer",
                        equalTo(recordProcessorCheckpointer)));
        assertThat(task, procTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.consumerState()));

        assertThat(
                state.shutdownTransition(ShutdownReason.LEASE_LOST),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.SHARD_END),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.consumerState()));

        assertThat(state.state(), equalTo(ShardConsumerState.PROCESSING));
        assertThat(state.taskType(), equalTo(TaskType.PROCESS));
    }

    @Test
    public void processingStateRecordsFetcher() {
        when(leaseCoordinator.leaseStatsRecorder()).thenReturn(leaseStatsRecorder);
        ConsumerState state = ShardConsumerState.PROCESSING.consumerState();
        ConsumerTask task = state.createTask(argument, consumer, null);

        assertThat(task, procTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, procTask(ShardRecordProcessor.class, "shardRecordProcessor", equalTo(shardRecordProcessor)));
        assertThat(
                task,
                procTask(
                        ShardRecordProcessorCheckpointer.class,
                        "recordProcessorCheckpointer",
                        equalTo(recordProcessorCheckpointer)));
        assertThat(task, procTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.consumerState()));

        assertThat(
                state.shutdownTransition(ShutdownReason.LEASE_LOST),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.SHARD_END),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.consumerState()));

        assertThat(state.state(), equalTo(ShardConsumerState.PROCESSING));
        assertThat(state.taskType(), equalTo(TaskType.PROCESS));
    }

    @Test
    public void shutdownRequestState() {
        ConsumerState state = ShardConsumerState.SHUTDOWN_REQUESTED.consumerState();

        consumer.gracefulShutdown(shutdownNotification);
        ConsumerTask task = state.createTask(argument, consumer, null);

        assertThat(
                task,
                shutdownReqTask(ShardRecordProcessor.class, "shardRecordProcessor", equalTo(shardRecordProcessor)));
        assertThat(
                task,
                shutdownReqTask(
                        RecordProcessorCheckpointer.class,
                        "recordProcessorCheckpointer",
                        equalTo(recordProcessorCheckpointer)));
        assertThat(
                task,
                shutdownReqTask(ShutdownNotification.class, "shutdownNotification", equalTo(shutdownNotification)));

        assertThat(state.successTransition(), equalTo(ConsumerStates.SHUTDOWN_REQUEST_COMPLETION_STATE));
        assertThat(
                state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ConsumerStates.SHUTDOWN_REQUEST_COMPLETION_STATE));
        assertThat(
                state.shutdownTransition(ShutdownReason.LEASE_LOST),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.SHARD_END),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));

        assertThat(state.state(), equalTo(ShardConsumerState.SHUTDOWN_REQUESTED));
        assertThat(state.taskType(), equalTo(TaskType.SHUTDOWN_NOTIFICATION));
    }

    @Test
    public void shutdownRequestCompleteStateTest() {
        ConsumerState state = ConsumerStates.SHUTDOWN_REQUEST_COMPLETION_STATE;

        assertThat(state.createTask(argument, consumer, null), nullValue());

        assertThat(state.successTransition(), equalTo(state));

        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED), equalTo(state));
        assertThat(
                state.shutdownTransition(ShutdownReason.LEASE_LOST),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));
        assertThat(
                state.shutdownTransition(ShutdownReason.SHARD_END),
                equalTo(ShardConsumerState.SHUTTING_DOWN.consumerState()));

        assertThat(state.state(), equalTo(ShardConsumerState.SHUTDOWN_REQUESTED));
        assertThat(state.taskType(), equalTo(TaskType.SHUTDOWN_NOTIFICATION));
    }

    @Test
    public void shuttingDownStateTest() {
        consumer.markForShutdown(ShutdownReason.SHARD_END);
        ConsumerState state = ShardConsumerState.SHUTTING_DOWN.consumerState();
        List<ChildShard> childShards = new ArrayList<>();
        List<String> parentShards = new ArrayList<>();
        parentShards.add("shardId-000000000000");
        ChildShard leftChild = ChildShard.builder()
                .shardId("shardId-000000000001")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("0", "49"))
                .build();
        ChildShard rightChild = ChildShard.builder()
                .shardId("shardId-000000000002")
                .parentShards(parentShards)
                .hashKeyRange(ShardObjectHelper.newHashKeyRange("50", "99"))
                .build();
        childShards.add(leftChild);
        childShards.add(rightChild);
        when(processRecordsInput.childShards()).thenReturn(childShards);
        ConsumerTask task = state.createTask(argument, consumer, processRecordsInput);

        assertThat(task, shutdownTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(
                task, shutdownTask(ShardRecordProcessor.class, "shardRecordProcessor", equalTo(shardRecordProcessor)));
        assertThat(
                task,
                shutdownTask(
                        ShardRecordProcessorCheckpointer.class,
                        "recordProcessorCheckpointer",
                        equalTo(recordProcessorCheckpointer)));
        assertThat(task, shutdownTask(ShutdownReason.class, "reason", equalTo(reason)));
        assertThat(task, shutdownTask(LeaseCoordinator.class, "leaseCoordinator", equalTo(leaseCoordinator)));
        assertThat(
                task,
                shutdownTask(Boolean.class, "cleanupLeasesOfCompletedShards", equalTo(cleanupLeasesOfCompletedShards)));
        assertThat(task, shutdownTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.SHUTDOWN_COMPLETE.consumerState()));

        for (ShutdownReason reason : ShutdownReason.values()) {
            assertThat(state.shutdownTransition(reason), equalTo(ShardConsumerState.SHUTDOWN_COMPLETE.consumerState()));
        }

        assertThat(state.state(), equalTo(ShardConsumerState.SHUTTING_DOWN));
        assertThat(state.taskType(), equalTo(TaskType.SHUTDOWN));
    }

    @Test
    public void shutdownCompleteStateTest() {
        consumer.gracefulShutdown(shutdownNotification);

        ConsumerState state = ShardConsumerState.SHUTDOWN_COMPLETE.consumerState();

        assertThat(state.createTask(argument, consumer, null), nullValue());

        assertThat(state.successTransition(), equalTo(state));
        for (ShutdownReason reason : ShutdownReason.values()) {
            assertThat(state.shutdownTransition(reason), equalTo(state));
        }

        assertThat(state.isTerminal(), equalTo(true));
        assertThat(state.state(), equalTo(ShardConsumerState.SHUTDOWN_COMPLETE));
        assertThat(state.taskType(), equalTo(TaskType.SHUTDOWN_COMPLETE));
    }

    static <ValueType> ReflectionPropertyMatcher<ShutdownTask, ValueType> shutdownTask(
            Class<ValueType> valueTypeClass, String propertyName, Matcher<ValueType> matcher) {
        return taskWith(ShutdownTask.class, valueTypeClass, propertyName, matcher);
    }

    static <ValueType> ReflectionPropertyMatcher<ShutdownNotificationTask, ValueType> shutdownReqTask(
            Class<ValueType> valueTypeClass, String propertyName, Matcher<ValueType> matcher) {
        return taskWith(ShutdownNotificationTask.class, valueTypeClass, propertyName, matcher);
    }

    static <ValueType> ReflectionPropertyMatcher<ProcessTask, ValueType> procTask(
            Class<ValueType> valueTypeClass, String propertyName, Matcher<ValueType> matcher) {
        return taskWith(ProcessTask.class, valueTypeClass, propertyName, matcher);
    }

    static <ValueType> ReflectionPropertyMatcher<InitializeTask, ValueType> initTask(
            Class<ValueType> valueTypeClass, String propertyName, Matcher<ValueType> matcher) {
        return taskWith(InitializeTask.class, valueTypeClass, propertyName, matcher);
    }

    static <TaskType, ValueType> ReflectionPropertyMatcher<TaskType, ValueType> taskWith(
            Class<TaskType> taskTypeClass,
            Class<ValueType> valueTypeClass,
            String propertyName,
            Matcher<ValueType> matcher) {
        return new ReflectionPropertyMatcher<>(taskTypeClass, valueTypeClass, matcher, propertyName);
    }

    private static class ReflectionPropertyMatcher<TaskType, ValueType>
            extends TypeSafeDiagnosingMatcher<ConsumerTask> {

        private final Class<TaskType> taskTypeClass;
        private final Class<ValueType> valueTypeClazz;
        private final Matcher<ValueType> matcher;
        private final String propertyName;
        private final Field matchingField;

        private ReflectionPropertyMatcher(
                Class<TaskType> taskTypeClass,
                Class<ValueType> valueTypeClass,
                Matcher<ValueType> matcher,
                String propertyName) {
            this.taskTypeClass = taskTypeClass;
            this.valueTypeClazz = valueTypeClass;
            this.matcher = matcher;
            this.propertyName = propertyName;

            Field[] fields = taskTypeClass.getDeclaredFields();
            Field matching = null;
            for (Field field : fields) {
                if (propertyName.equals(field.getName())) {
                    matching = field;
                }
            }
            this.matchingField = matching;
        }

        @Override
        protected boolean matchesSafely(ConsumerTask item, Description mismatchDescription) {

            return Condition.matched(item, mismatchDescription)
                    .and(new Condition.Step<ConsumerTask, TaskType>() {
                        @Override
                        public Condition<TaskType> apply(ConsumerTask value, Description mismatch) {
                            if (taskTypeClass.equals(value.getClass())) {
                                return Condition.matched(taskTypeClass.cast(value), mismatch);
                            }
                            mismatch.appendText("Expected task type of ")
                                    .appendText(taskTypeClass.getName())
                                    .appendText(" but was ")
                                    .appendText(value.getClass().getName());
                            return Condition.notMatched();
                        }
                    })
                    .and(new Condition.Step<TaskType, Object>() {
                        @Override
                        public Condition<Object> apply(TaskType value, Description mismatch) {
                            if (matchingField == null) {
                                mismatch.appendText("Field ")
                                        .appendText(propertyName)
                                        .appendText(" not present in ")
                                        .appendText(taskTypeClass.getName());
                                return Condition.notMatched();
                            }

                            try {
                                return Condition.matched(getValue(value), mismatch);
                            } catch (RuntimeException re) {
                                mismatch.appendText("Failure while retrieving value for ")
                                        .appendText(propertyName);
                                return Condition.notMatched();
                            }
                        }
                    })
                    .and(new Condition.Step<Object, ValueType>() {
                        @Override
                        public Condition<ValueType> apply(Object value, Description mismatch) {
                            if (value != null && !valueTypeClazz.isAssignableFrom(value.getClass())) {
                                mismatch.appendText("Expected a value of type ")
                                        .appendText(valueTypeClazz.getName())
                                        .appendText(" but was ")
                                        .appendText(value.getClass().getName());
                                return Condition.notMatched();
                            }
                            return Condition.matched(valueTypeClazz.cast(value), mismatch);
                        }
                    })
                    .matching(matcher);
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText(
                            "A " + taskTypeClass.getName() + " task with the property " + propertyName + " matching ")
                    .appendDescriptionOf(matcher);
        }

        private Object getValue(TaskType task) {

            matchingField.setAccessible(true);
            try {
                return matchingField.get(task);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to retrieve the value for " + matchingField.getName());
            }
        }
    }
}
