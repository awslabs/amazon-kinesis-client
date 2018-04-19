/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.kinesis.lifecycle;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.lifecycle.ConsumerStates.ConsumerState;
import static software.amazon.kinesis.lifecycle.ConsumerStates.ShardConsumerState;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.hamcrest.Condition;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;

import software.amazon.kinesis.checkpoint.RecordProcessorCheckpointer;
import software.amazon.kinesis.leases.LeaseManager;
import software.amazon.kinesis.leases.KinesisClientLease;
import software.amazon.kinesis.leases.LeaseManagerProxy;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.IMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.RecordProcessor;
import software.amazon.kinesis.processor.IRecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.GetRecordsCache;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerStatesTest {
    private static final String STREAM_NAME = "TestStream";
    private static final InitialPositionInStreamExtended INITIAL_POSITION_IN_STREAM =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    private ShardConsumer consumer;

    @Mock
    private RecordProcessor recordProcessor;
    @Mock
    private RecordProcessorCheckpointer recordProcessorCheckpointer;
    @Mock
    private ExecutorService executorService;
    @Mock
    private ShardInfo shardInfo;
    @Mock
    private LeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private Checkpointer checkpoint;
    @Mock
    private ShutdownNotification shutdownNotification;
    @Mock
    private InitialPositionInStreamExtended initialPositionInStream;
    @Mock
    private GetRecordsCache getRecordsCache;
    @Mock
    private AmazonKinesis amazonKinesis;
    @Mock
    private LeaseManagerProxy leaseManagerProxy;
    @Mock
    private IMetricsFactory metricsFactory;

    private long parentShardPollIntervalMillis = 0xCAFE;
    private boolean cleanupLeasesOfCompletedShards = true;
    private long taskBackoffTimeMillis = 0xBEEF;
    private ShutdownReason reason = ShutdownReason.TERMINATE;
    private boolean skipShardSyncAtWorkerInitializationIfLeasesExist = true;
    private long listShardsBackoffTimeInMillis = 50L;
    private int maxListShardsRetryAttempts = 10;
    private boolean shouldCallProcessRecordsEvenForEmptyRecordList = true;
    private boolean ignoreUnexpectedChildShards = false;
    private long idleTimeInMillis = 1000L;


    @Before
    public void setup() {
        consumer = spy(new ShardConsumer(shardInfo, STREAM_NAME, leaseManager, executorService, getRecordsCache,
                recordProcessor, checkpoint, recordProcessorCheckpointer, parentShardPollIntervalMillis,
                taskBackoffTimeMillis, Optional.empty(), amazonKinesis,
                skipShardSyncAtWorkerInitializationIfLeasesExist, listShardsBackoffTimeInMillis,
                maxListShardsRetryAttempts, shouldCallProcessRecordsEvenForEmptyRecordList, idleTimeInMillis,
                INITIAL_POSITION_IN_STREAM, cleanupLeasesOfCompletedShards, ignoreUnexpectedChildShards,
                leaseManagerProxy, metricsFactory));

        when(shardInfo.shardId()).thenReturn("shardId-000000000000");
    }

    private static final Class<LeaseManager<KinesisClientLease>> LEASE_MANAGER_CLASS = (Class<LeaseManager<KinesisClientLease>>) (Class<?>) LeaseManager.class;

    @Test
    public void blockOnParentStateTest() {
        ConsumerState state = ShardConsumerState.WAITING_ON_PARENT_SHARDS.getConsumerState();

        ITask task = state.createTask(consumer);

        assertThat(task, taskWith(BlockOnParentShardTask.class, ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task,
                taskWith(BlockOnParentShardTask.class, LEASE_MANAGER_CLASS, "leaseManager", equalTo(leaseManager)));
        assertThat(task, taskWith(BlockOnParentShardTask.class, Long.class, "parentShardPollIntervalMillis",
                equalTo(parentShardPollIntervalMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.INITIALIZING.getConsumerState()));
        for (ShutdownReason shutdownReason : ShutdownReason.values()) {
            assertThat(state.shutdownTransition(shutdownReason),
                    equalTo(ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState()));
        }

        assertThat(state.getState(), equalTo(ShardConsumerState.WAITING_ON_PARENT_SHARDS));
        assertThat(state.getTaskType(), equalTo(TaskType.BLOCK_ON_PARENT_SHARDS));

    }

    @Test
    public void initializingStateTest() {
        ConsumerState state = ShardConsumerState.INITIALIZING.getConsumerState();
        ITask task = state.createTask(consumer);

        assertThat(task, initTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, initTask(RecordProcessor.class, "recordProcessor", equalTo(recordProcessor)));
        assertThat(task, initTask(Checkpointer.class, "checkpoint", equalTo(checkpoint)));
        assertThat(task, initTask(RecordProcessorCheckpointer.class, "recordProcessorCheckpointer",
                equalTo(recordProcessorCheckpointer)));
        assertThat(task, initTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.getConsumerState()));

        assertThat(state.shutdownTransition(ShutdownReason.ZOMBIE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.TERMINATE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState()));

        assertThat(state.getState(), equalTo(ShardConsumerState.INITIALIZING));
        assertThat(state.getTaskType(), equalTo(TaskType.INITIALIZE));
    }

    @Test
    public void processingStateTestSynchronous() {
        when(getRecordsCache.getNextResult()).thenReturn(new ProcessRecordsInput());

        ConsumerState state = ShardConsumerState.PROCESSING.getConsumerState();
        ITask task = state.createTask(consumer);

        assertThat(task, procTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, procTask(RecordProcessor.class, "recordProcessor", equalTo(recordProcessor)));
        assertThat(task, procTask(RecordProcessorCheckpointer.class, "recordProcessorCheckpointer",
                equalTo(recordProcessorCheckpointer)));
        assertThat(task, procTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.getConsumerState()));

        assertThat(state.shutdownTransition(ShutdownReason.ZOMBIE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.TERMINATE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState()));

        assertThat(state.getState(), equalTo(ShardConsumerState.PROCESSING));
        assertThat(state.getTaskType(), equalTo(TaskType.PROCESS));

    }

    @Test
    public void processingStateTestAsynchronous() {
        when(getRecordsCache.getNextResult()).thenReturn(new ProcessRecordsInput());

        ConsumerState state = ShardConsumerState.PROCESSING.getConsumerState();
        ITask task = state.createTask(consumer);

        assertThat(task, procTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, procTask(RecordProcessor.class, "recordProcessor", equalTo(recordProcessor)));
        assertThat(task, procTask(RecordProcessorCheckpointer.class, "recordProcessorCheckpointer",
                equalTo(recordProcessorCheckpointer)));
        assertThat(task, procTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.getConsumerState()));

        assertThat(state.shutdownTransition(ShutdownReason.ZOMBIE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.TERMINATE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState()));

        assertThat(state.getState(), equalTo(ShardConsumerState.PROCESSING));
        assertThat(state.getTaskType(), equalTo(TaskType.PROCESS));

    }

    @Test
    public void processingStateRecordsFetcher() {
        when(getRecordsCache.getNextResult()).thenReturn(new ProcessRecordsInput());

        ConsumerState state = ShardConsumerState.PROCESSING.getConsumerState();
        ITask task = state.createTask(consumer);

        assertThat(task, procTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, procTask(RecordProcessor.class, "recordProcessor", equalTo(recordProcessor)));
        assertThat(task, procTask(RecordProcessorCheckpointer.class, "recordProcessorCheckpointer",
                equalTo(recordProcessorCheckpointer)));
        assertThat(task, procTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.PROCESSING.getConsumerState()));

        assertThat(state.shutdownTransition(ShutdownReason.ZOMBIE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.TERMINATE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState()));

        assertThat(state.getState(), equalTo(ShardConsumerState.PROCESSING));
        assertThat(state.getTaskType(), equalTo(TaskType.PROCESS));
    }

    @Test
    public void shutdownRequestState() {
        ConsumerState state = ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState();

        consumer.notifyShutdownRequested(shutdownNotification);
        ITask task = state.createTask(consumer);

        assertThat(task, shutdownReqTask(RecordProcessor.class, "recordProcessor", equalTo(recordProcessor)));
        assertThat(task, shutdownReqTask(IRecordProcessorCheckpointer.class, "recordProcessorCheckpointer",
                equalTo(recordProcessorCheckpointer)));
        assertThat(task, shutdownReqTask(ShutdownNotification.class, "shutdownNotification", equalTo(shutdownNotification)));

        assertThat(state.successTransition(), equalTo(ConsumerStates.SHUTDOWN_REQUEST_COMPLETION_STATE));
        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED),
                equalTo(ConsumerStates.SHUTDOWN_REQUEST_COMPLETION_STATE));
        assertThat(state.shutdownTransition(ShutdownReason.ZOMBIE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.TERMINATE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));

        assertThat(state.getState(), equalTo(ShardConsumerState.SHUTDOWN_REQUESTED));
        assertThat(state.getTaskType(), equalTo(TaskType.SHUTDOWN_NOTIFICATION));

    }

    @Test
    public void shutdownRequestCompleteStateTest() {
        ConsumerState state = ConsumerStates.SHUTDOWN_REQUEST_COMPLETION_STATE;

        assertThat(state.createTask(consumer), nullValue());

        assertThat(state.successTransition(), equalTo(state));

        assertThat(state.shutdownTransition(ShutdownReason.REQUESTED), equalTo(state));
        assertThat(state.shutdownTransition(ShutdownReason.ZOMBIE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));
        assertThat(state.shutdownTransition(ShutdownReason.TERMINATE),
                equalTo(ShardConsumerState.SHUTTING_DOWN.getConsumerState()));

        assertThat(state.getState(), equalTo(ShardConsumerState.SHUTDOWN_REQUESTED));
        assertThat(state.getTaskType(), equalTo(TaskType.SHUTDOWN_NOTIFICATION));

    }

    // TODO: Fix this test
    @Ignore
    @Test
    public void shuttingDownStateTest() {
        consumer.markForShutdown(ShutdownReason.TERMINATE);
        ConsumerState state = ShardConsumerState.SHUTTING_DOWN.getConsumerState();
        ITask task = state.createTask(consumer);

        assertThat(task, shutdownTask(ShardInfo.class, "shardInfo", equalTo(shardInfo)));
        assertThat(task, shutdownTask(RecordProcessor.class, "recordProcessor", equalTo(recordProcessor)));
        assertThat(task, shutdownTask(RecordProcessorCheckpointer.class, "recordProcessorCheckpointer",
                equalTo(recordProcessorCheckpointer)));
        assertThat(task, shutdownTask(ShutdownReason.class, "reason", equalTo(reason)));
        assertThat(task, shutdownTask(LEASE_MANAGER_CLASS, "leaseManager", equalTo(leaseManager)));
        assertThat(task, shutdownTask(InitialPositionInStreamExtended.class, "initialPositionInStream",
                equalTo(initialPositionInStream)));
        assertThat(task,
                shutdownTask(Boolean.class, "cleanupLeasesOfCompletedShards", equalTo(cleanupLeasesOfCompletedShards)));
        assertThat(task, shutdownTask(Long.class, "backoffTimeMillis", equalTo(taskBackoffTimeMillis)));

        assertThat(state.successTransition(), equalTo(ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState()));

        for (ShutdownReason reason : ShutdownReason.values()) {
            assertThat(state.shutdownTransition(reason),
                    equalTo(ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState()));
        }

        assertThat(state.getState(), equalTo(ShardConsumerState.SHUTTING_DOWN));
        assertThat(state.getTaskType(), equalTo(TaskType.SHUTDOWN));

    }

    @Test
    public void shutdownCompleteStateTest() {
        consumer.notifyShutdownRequested(shutdownNotification);

        ConsumerState state = ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState();

        assertThat(state.createTask(consumer), nullValue());
        verify(consumer, times(2)).shutdownNotification();
        verify(shutdownNotification).shutdownComplete();

        assertThat(state.successTransition(), equalTo(state));
        for(ShutdownReason reason : ShutdownReason.values()) {
            assertThat(state.shutdownTransition(reason), equalTo(state));
        }

        assertThat(state.getState(), equalTo(ShardConsumerState.SHUTDOWN_COMPLETE));
        assertThat(state.getTaskType(), equalTo(TaskType.SHUTDOWN_COMPLETE));
    }

    @Test
    public void shutdownCompleteStateNullNotificationTest() {
        ConsumerState state = ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState();

        when(consumer.shutdownNotification()).thenReturn(null);
        assertThat(state.createTask(consumer), nullValue());

        verify(consumer).shutdownNotification();
        verify(shutdownNotification, never()).shutdownComplete();
    }

    static <ValueType> ReflectionPropertyMatcher<ShutdownTask, ValueType> shutdownTask(Class<ValueType> valueTypeClass,
                                                                                       String propertyName, Matcher<ValueType> matcher) {
        return taskWith(ShutdownTask.class, valueTypeClass, propertyName, matcher);
    }

    static <ValueType> ReflectionPropertyMatcher<ShutdownNotificationTask, ValueType> shutdownReqTask(
            Class<ValueType> valueTypeClass, String propertyName, Matcher<ValueType> matcher) {
        return taskWith(ShutdownNotificationTask.class, valueTypeClass, propertyName, matcher);
    }

    static <ValueType> ReflectionPropertyMatcher<ProcessTask, ValueType> procTask(Class<ValueType> valueTypeClass,
                                                                                  String propertyName, Matcher<ValueType> matcher) {
        return taskWith(ProcessTask.class, valueTypeClass, propertyName, matcher);
    }

    static <ValueType> ReflectionPropertyMatcher<InitializeTask, ValueType> initTask(Class<ValueType> valueTypeClass,
                                                                                     String propertyName, Matcher<ValueType> matcher) {
        return taskWith(InitializeTask.class, valueTypeClass, propertyName, matcher);
    }

    static <TaskType, ValueType> ReflectionPropertyMatcher<TaskType, ValueType> taskWith(Class<TaskType> taskTypeClass,
                                                                                         Class<ValueType> valueTypeClass, String propertyName, Matcher<ValueType> matcher) {
        return new ReflectionPropertyMatcher<>(taskTypeClass, valueTypeClass, matcher, propertyName);
    }

    private static class ReflectionPropertyMatcher<TaskType, ValueType> extends TypeSafeDiagnosingMatcher<ITask> {

        private final Class<TaskType> taskTypeClass;
        private final Class<ValueType> valueTypeClazz;
        private final Matcher<ValueType> matcher;
        private final String propertyName;
        private final Field matchingField;

        private ReflectionPropertyMatcher(Class<TaskType> taskTypeClass, Class<ValueType> valueTypeClass,
                                          Matcher<ValueType> matcher, String propertyName) {
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
        protected boolean matchesSafely(ITask item, Description mismatchDescription) {

            return Condition.matched(item, mismatchDescription).and(new Condition.Step<ITask, TaskType>() {
                @Override
                public Condition<TaskType> apply(ITask value, Description mismatch) {
                    if (taskTypeClass.equals(value.getClass())) {
                        return Condition.matched(taskTypeClass.cast(value), mismatch);
                    }
                    mismatch.appendText("Expected task type of ").appendText(taskTypeClass.getName())
                            .appendText(" but was ").appendText(value.getClass().getName());
                    return Condition.notMatched();
                }
            }).and(new Condition.Step<TaskType, Object>() {
                @Override
                public Condition<Object> apply(TaskType value, Description mismatch) {
                    if (matchingField == null) {
                        mismatch.appendText("Field ").appendText(propertyName).appendText(" not present in ")
                                .appendText(taskTypeClass.getName());
                        return Condition.notMatched();
                    }

                    try {
                        return Condition.matched(getValue(value), mismatch);
                    } catch (RuntimeException re) {
                        mismatch.appendText("Failure while retrieving value for ").appendText(propertyName);
                        return Condition.notMatched();
                    }

                }
            }).and(new Condition.Step<Object, ValueType>() {
                @Override
                public Condition<ValueType> apply(Object value, Description mismatch) {
                    if (value != null && !valueTypeClazz.isAssignableFrom(value.getClass())) {
                        mismatch.appendText("Expected a value of type ").appendText(valueTypeClazz.getName())
                                .appendText(" but was ").appendText(value.getClass().getName());
                        return Condition.notMatched();
                    }
                    return Condition.matched(valueTypeClazz.cast(value), mismatch);
                }
            }).matching(matcher);
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