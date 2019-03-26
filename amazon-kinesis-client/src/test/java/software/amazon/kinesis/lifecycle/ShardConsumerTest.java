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

package software.amazon.kinesis.lifecycle;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Unit tests of {@link ShardConsumer}.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class ShardConsumerTest {

    private ShardConsumerTestContext ctx = new ShardConsumerTestContext();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        ctx = new ShardConsumerTestContext();
        ctx.setShardInfo(
                new ShardInfo(ctx.getShardId(), ctx.getConcurrencyToken(), null, ExtendedSequenceNumber.TRIM_HORIZON));
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("test-" + testName.getMethodName() + "-%04d")
                .setDaemon(true).build();
        ctx.setExecutorService(new ThreadPoolExecutor(4, 4, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), factory));

        ctx.setProcessRecordsInput(ProcessRecordsInput.builder().isAtShardEnd(false).cacheEntryTime(Instant.now())
                .millisBehindLatest(1000L).records(Collections.emptyList()).build());
        ctx.setInitialTaskInput(TaskExecutionListenerInput.builder().shardInfo(ctx.getShardInfo())
                .taskType(TaskType.INITIALIZE).build());
        ctx.setProcessTaskInput(
                TaskExecutionListenerInput.builder().shardInfo(ctx.getShardInfo()).taskType(TaskType.PROCESS).build());
        ctx.setShutdownRequestedTaskInput(TaskExecutionListenerInput.builder().shardInfo(ctx.getShardInfo())
                .taskType(TaskType.SHUTDOWN_NOTIFICATION).build());
        ctx.setShutdownRequestedAwaitTaskInput(TaskExecutionListenerInput.builder().shardInfo(ctx.getShardInfo())
                .taskType(TaskType.SHUTDOWN_COMPLETE).build());
        ctx.setShutdownTaskInput(
                TaskExecutionListenerInput.builder().shardInfo(ctx.getShardInfo()).taskType(TaskType.SHUTDOWN).build());
    }

    @After
    public void after() {
        List<Runnable> remainder = ctx.getExecutorService().shutdownNow();
        assertThat(remainder.isEmpty(), equalTo(true));
    }

    private class TestPublisher implements RecordsPublisher {

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CyclicBarrier requestBarrier = new CyclicBarrier(2);

        Subscriber<? super RecordsRetrieved> subscriber;
        final Subscription subscription = mock(Subscription.class);

        TestPublisher() {
            this(false);
        }

        TestPublisher(boolean enableCancelAwait) {
            doAnswer(a -> {
                requestBarrier.await();
                return null;
            }).when(subscription).request(anyLong());
            doAnswer(a -> {
                if (enableCancelAwait) {
                    requestBarrier.await();
                }
                return null;
            }).when(subscription).cancel();
        }

        @Override
        public void start(ExtendedSequenceNumber extendedSequenceNumber,
                InitialPositionInStreamExtended initialPositionInStreamExtended) {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public void subscribe(Subscriber<? super RecordsRetrieved> s) {
            subscriber = s;
            subscriber.onSubscribe(subscription);
            try {
                barrier.await();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void restartFrom(RecordsRetrieved recordsRetrieved) {

        }

        public void awaitSubscription() throws InterruptedException, BrokenBarrierException {
            barrier.await();
            barrier.reset();
        }

        public void awaitRequest() throws InterruptedException, BrokenBarrierException {
            requestBarrier.await();
            requestBarrier.reset();
        }

        public void awaitInitialSetup() throws InterruptedException, BrokenBarrierException {
            awaitRequest();
            awaitSubscription();
        }

        public void publish() {
            publish(() -> ctx.getProcessRecordsInput());
        }

        public void publish(RecordsRetrieved input) {
            subscriber.onNext(input);
        }
    }

    @Test
    public void simpleTest() throws Exception {
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskCallBarrier);

        mockSuccessfulShutdown(null);

        TestPublisher cache = new TestPublisher();
        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(),
                ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(), ctx.getInitialState(),
                Function.identity(), 1, ctx.getTaskExecutionListener(), 0);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        consumer.leaseLost();
        boolean shutdownComplete = consumer.shutdownComplete().get();
        while (!shutdownComplete) {
            shutdownComplete = consumer.shutdownComplete().get();
        }

        verify(cache.subscription, times(3)).request(anyLong());
        verify(cache.subscription).cancel();
        verify(ctx.getProcessingState(), times(2)).createTask(eq(ctx.getShardConsumerArgument()), eq(consumer), any());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(2)).beforeTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownTaskInput());

        ctx.setInitialTaskInput(ctx.getInitialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setProcessTaskInput(ctx.getProcessTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setShutdownTaskInput(ctx.getShutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(2)).afterTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    @Test(timeout = 1000L)
    public void testLeaseLossIsNonBlocking() throws Exception {
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);
        CyclicBarrier processingTaskInterlock = new CyclicBarrier(2);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskCallBarrier, processingTaskInterlock);

        mockSuccessfulShutdown(null);

        TestPublisher cache = new TestPublisher();
        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(),
                ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(), ctx.getInitialState(),
                Function.identity(), 1, ctx.getTaskExecutionListener(), 0);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        log.debug("Setup complete publishing entry");
        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        consumer.leaseLost();

        //
        // This will block if a lock is held on ShardConsumer#this
        //
        consumer.executeLifecycle();
        assertThat(consumer.isShutdown(), equalTo(false));

        log.debug("Release processing task interlock");
        awaitAndResetBarrier(processingTaskInterlock);

        while (!consumer.isShutdown()) {
            consumer.executeLifecycle();
            Thread.yield();
        }

        verify(cache.subscription, times(1)).request(anyLong());
        verify(cache.subscription).cancel();
        verify(ctx.getProcessingState(), times(1)).createTask(eq(ctx.getShardConsumerArgument()), eq(consumer), any());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownTaskInput());

        ctx.setInitialTaskInput(ctx.getInitialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setProcessTaskInput(ctx.getProcessTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setShutdownTaskInput(ctx.getShutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    @Test
    public void testDataArrivesAfterProcessing2() throws Exception {

        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskCallBarrier);

        mockSuccessfulShutdown(null);

        TestPublisher cache = new TestPublisher();
        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(),
                ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(), ctx.getInitialState(),
                Function.identity(), 1, ctx.getTaskExecutionListener(), 0);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);

        verify(ctx.getProcessingState()).createTask(any(), any(), any());
        verify(ctx.getProcessingTask()).call();

        cache.awaitRequest();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        verify(ctx.getProcessingState(), times(2)).createTask(any(), any(), any());
        verify(ctx.getProcessingTask(), times(2)).call();

        cache.awaitRequest();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        log.info("Starting shutdown");
        consumer.leaseLost();
        boolean shutdownComplete;
        do {
            shutdownComplete = consumer.shutdownComplete().get();
        } while (!shutdownComplete);

        verify(ctx.getProcessingState(), times(3)).createTask(any(), any(), any());
        verify(ctx.getProcessingTask(), times(3)).call();
        verify(ctx.getProcessingState()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(ctx.getShutdownState()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(3)).beforeTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownTaskInput());

        ctx.setInitialTaskInput(ctx.getInitialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setProcessTaskInput(ctx.getProcessTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setShutdownTaskInput(ctx.getShutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(3)).afterTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    @SuppressWarnings("unchecked")
    @Test
    @Ignore
    public final void testInitializationStateUponFailure() throws Exception {
        ShardConsumer consumer = new ShardConsumer(ctx.getRecordsPublisher(), ctx.getExecutorService(),
                ctx.getShardInfo(), ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(),
                ctx.getInitialState(), Function.identity(), 1, ctx.getTaskExecutionListener(), 0);

        when(ctx.getInitialState().createTask(eq(ctx.getShardConsumerArgument()), eq(consumer), any()))
                .thenReturn(ctx.getInitializeTask());
        when(ctx.getInitializeTask().call()).thenReturn(new TaskResult(new Exception("Bad")));
        when(ctx.getInitializeTask().taskType()).thenReturn(TaskType.INITIALIZE);
        when(ctx.getInitialState().failureTransition()).thenReturn(ctx.getInitialState());

        CyclicBarrier taskBarrier = new CyclicBarrier(2);

        when(ctx.getInitialState().requiresDataAvailability()).thenAnswer(i -> {
            taskBarrier.await();
            return false;
        });

        consumer.executeLifecycle();
        for (int i = 0; i < 4; ++i) {
            awaitAndResetBarrier(taskBarrier);
        }

        verify(ctx.getInitialState(), times(5)).createTask(eq(ctx.getShardConsumerArgument()), eq(consumer), any());
        verify(ctx.getInitialState(), never()).successTransition();
        verify(ctx.getInitialState(), never()).shutdownTransition(any());
    }

    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test(expected = RejectedExecutionException.class)
    public final void testInitializationStateUponSubmissionFailure() throws Exception {

        ExecutorService failingService = mock(ExecutorService.class);
        ShardConsumer consumer = new ShardConsumer(ctx.getRecordsPublisher(), failingService, ctx.getShardInfo(),
                ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(), ctx.getInitialState(), t -> t, 1,
                ctx.getTaskExecutionListener(), 0);

        doThrow(new RejectedExecutionException()).when(failingService).execute(any());

        boolean initComplete;
        do {
            initComplete = consumer.initializeComplete().get();
        } while (!initComplete);
        verifyZeroInteractions(ctx.getTaskExecutionListener());
    }

    @Test
    public void testErrorThrowableInInitialization() throws Exception {
        ShardConsumer consumer = new ShardConsumer(ctx.getRecordsPublisher(), ctx.getExecutorService(),
                ctx.getShardInfo(), ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(),
                ctx.getInitialState(), t -> t, 1, ctx.getTaskExecutionListener(), 0);

        when(ctx.getInitialState().createTask(any(), any(), any())).thenReturn(ctx.getInitializeTask());
        when(ctx.getInitialState().taskType()).thenReturn(TaskType.INITIALIZE);
        when(ctx.getInitializeTask().call()).thenAnswer(i -> {
            throw new Error("Error");
        });

        try {
            consumer.initializeComplete().get();
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(Error.class));
        }
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    @Test
    public void testRequestedShutdownWhileQuiet() throws Exception {

        CyclicBarrier taskBarrier = new CyclicBarrier(2);

        TestPublisher cache = new TestPublisher();
        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(),
                ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(), ctx.getInitialState(), t -> t, 1,
                ctx.getTaskExecutionListener(), 0);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskBarrier);

        when(ctx.getProcessingState().shutdownTransition(eq(ShutdownReason.REQUESTED)))
                .thenReturn(ctx.getShutdownRequestedState());
        when(ctx.getShutdownRequestedState().requiresDataAvailability()).thenReturn(false);
        when(ctx.getShutdownRequestedState().createTask(any(), any(), any()))
                .thenReturn(ctx.getShutdownRequestedTask());
        when(ctx.getShutdownRequestedState().taskType()).thenReturn(TaskType.SHUTDOWN_NOTIFICATION);
        when(ctx.getShutdownRequestedTask().call()).thenReturn(new TaskResult(null));

        when(ctx.getShutdownRequestedState().shutdownTransition(eq(ShutdownReason.REQUESTED)))
                .thenReturn(ctx.getShutdownRequestedAwaitState());
        when(ctx.getShutdownRequestedState().shutdownTransition(eq(ShutdownReason.LEASE_LOST)))
                .thenReturn(ctx.getShutdownState());
        when(ctx.getShutdownRequestedAwaitState().requiresDataAvailability()).thenReturn(false);
        when(ctx.getShutdownRequestedAwaitState().createTask(any(), any(), any())).thenReturn(null);
        when(ctx.getShutdownRequestedAwaitState().shutdownTransition(eq(ShutdownReason.REQUESTED)))
                .thenReturn(ctx.getShutdownRequestedState());
        when(ctx.getShutdownRequestedAwaitState().shutdownTransition(eq(ShutdownReason.LEASE_LOST)))
                .thenReturn(ctx.getShutdownState());
        when(ctx.getShutdownRequestedAwaitState().taskType()).thenReturn(TaskType.SHUTDOWN_COMPLETE);

        mockSuccessfulShutdown(null);

        boolean init = consumer.initializeComplete().get();
        while (!init) {
            init = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        awaitAndResetBarrier(taskBarrier);
        cache.awaitRequest();

        cache.publish();
        awaitAndResetBarrier(taskBarrier);
        cache.awaitRequest();

        consumer.gracefulShutdown(ctx.getShutdownNotification());
        boolean shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(false));
        shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(false));

        consumer.leaseLost();
        shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(false));
        shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(true));

        verify(ctx.getProcessingState(), times(2)).createTask(any(), any(), any());
        verify(ctx.getShutdownRequestedState(), never()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(ctx.getShutdownRequestedState()).createTask(any(), any(), any());
        verify(ctx.getShutdownRequestedState()).shutdownTransition(eq(ShutdownReason.REQUESTED));
        verify(ctx.getShutdownRequestedAwaitState()).createTask(any(), any(), any());
        verify(ctx.getShutdownRequestedAwaitState()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(2)).beforeTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownRequestedTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownRequestedAwaitTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownTaskInput());

        ctx.setInitialTaskInput(ctx.getInitialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setProcessTaskInput(ctx.getProcessTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setShutdownRequestedTaskInput(
                ctx.getShutdownRequestedTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setShutdownTaskInput(ctx.getShutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        // No task is created/run for this shutdownRequestedAwaitState, so there's no task outcome.

        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(2)).afterTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownRequestedTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownRequestedAwaitTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    @Test
    public void testExceptionInProcessingStopsRequests() throws Exception {
        TestPublisher cache = new TestPublisher();

        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(), Optional.of(1L),
                ctx.getShardConsumerArgument(), ctx.getInitialState(), Function.identity(), 1,
                ctx.getTaskExecutionListener(), 0);

        mockSuccessfulInitialize(null);
        mockSuccessfulProcessing(null);

        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);
        final RuntimeException expectedException = new RuntimeException("Whee");
        when(ctx.getProcessingTask().call()).thenAnswer(a -> {
            try {
                throw expectedException;
            } finally {
                taskCallBarrier.await();
            }
        });

        boolean initComplete;
        do {
            initComplete = consumer.initializeComplete().get();
        } while (!initComplete);

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        Throwable healthCheckOutcome = consumer.healthCheck();

        assertThat(healthCheckOutcome, equalTo(expectedException));

        verify(cache.subscription, times(2)).request(anyLong());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getProcessTaskInput());

        ctx.setInitialTaskInput(ctx.getInitialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getInitialTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    @Test
    public void testLongRunningTasks() throws Exception {

        TestPublisher cache = new TestPublisher();

        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(), Optional.of(1L),
                ctx.getShardConsumerArgument(), ctx.getInitialState(), Function.identity(), 1,
                ctx.getTaskExecutionListener(), 0);

        CyclicBarrier taskArriveBarrier = new CyclicBarrier(2);
        CyclicBarrier taskDepartBarrier = new CyclicBarrier(2);

        mockSuccessfulInitialize(taskArriveBarrier, taskDepartBarrier);
        mockSuccessfulProcessing(taskArriveBarrier, taskDepartBarrier);
        mockSuccessfulShutdown(taskArriveBarrier, taskDepartBarrier);

        CompletableFuture<Boolean> initSuccess = consumer.initializeComplete();

        awaitAndResetBarrier(taskArriveBarrier);
        assertThat(consumer.taskRunningTime(), notNullValue());
        consumer.healthCheck();
        awaitAndResetBarrier(taskDepartBarrier);

        assertThat(initSuccess.get(), equalTo(false));
        verify(ctx.getInitializeTask()).call();

        initSuccess = consumer.initializeComplete();
        verify(ctx.getInitializeTask()).call();
        assertThat(initSuccess.get(), equalTo(true));
        consumer.healthCheck();

        assertThat(consumer.taskRunningTime(), nullValue());

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();

        awaitAndResetBarrier(taskArriveBarrier);
        Instant previousTaskStartTime = consumer.taskDispatchedAt();
        assertThat(consumer.taskRunningTime(), notNullValue());
        consumer.healthCheck();
        awaitAndResetBarrier(taskDepartBarrier);

        consumer.healthCheck();

        cache.requestBarrier.await();
        assertThat(consumer.taskRunningTime(), nullValue());
        cache.requestBarrier.reset();

        cache.publish();

        awaitAndResetBarrier(taskArriveBarrier);
        Instant currentTaskStartTime = consumer.taskDispatchedAt();
        assertThat(currentTaskStartTime, not(equalTo(previousTaskStartTime)));
        awaitAndResetBarrier(taskDepartBarrier);

        cache.requestBarrier.await();
        assertThat(consumer.taskRunningTime(), nullValue());
        cache.requestBarrier.reset();

        consumer.leaseLost();

        assertThat(consumer.isShutdownRequested(), equalTo(true));
        CompletableFuture<Boolean> shutdownComplete = consumer.shutdownComplete();

        awaitAndResetBarrier(taskArriveBarrier);
        assertThat(consumer.taskRunningTime(), notNullValue());
        awaitAndResetBarrier(taskDepartBarrier);

        assertThat(shutdownComplete.get(), equalTo(false));

        shutdownComplete = consumer.shutdownComplete();
        assertThat(shutdownComplete.get(), equalTo(true));

        assertThat(consumer.taskRunningTime(), nullValue());
        consumer.healthCheck();

        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(2)).beforeTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).beforeTaskExecution(ctx.getShutdownTaskInput());

        ctx.setInitialTaskInput(ctx.getInitialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setProcessTaskInput(ctx.getProcessTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        ctx.setShutdownTaskInput(ctx.getShutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getInitialTaskInput());
        verify(ctx.getTaskExecutionListener(), times(2)).afterTaskExecution(ctx.getProcessTaskInput());
        verify(ctx.getTaskExecutionListener(), times(1)).afterTaskExecution(ctx.getShutdownTaskInput());
        verifyNoMoreInteractions(ctx.getTaskExecutionListener());
    }

    private void mockSuccessfulShutdown(CyclicBarrier taskCallBarrier) {
        mockSuccessfulShutdown(taskCallBarrier, null);
    }

    private void mockSuccessfulShutdown(CyclicBarrier taskArriveBarrier, CyclicBarrier taskDepartBarrier) {
        when(ctx.getShutdownState().createTask(eq(ctx.getShardConsumerArgument()), any(), any()))
                .thenReturn(ctx.getShutdownTask());
        when(ctx.getShutdownState().taskType()).thenReturn(TaskType.SHUTDOWN);
        when(ctx.getShutdownTask().taskType()).thenReturn(TaskType.SHUTDOWN);
        when(ctx.getShutdownTask().call()).thenAnswer(i -> {
            awaitBarrier(taskArriveBarrier);
            awaitBarrier(taskDepartBarrier);
            return new TaskResult(null);
        });
        when(ctx.getShutdownState().shutdownTransition(any())).thenReturn(ctx.getShutdownCompleteState());
        when(ctx.getShutdownState().state()).thenReturn(ConsumerStates.ShardConsumerState.SHUTTING_DOWN);

        when(ctx.getShutdownCompleteState().isTerminal()).thenReturn(true);
    }

    private void mockSuccessfulProcessing(CyclicBarrier taskCallBarrier) {
        mockSuccessfulProcessing(taskCallBarrier, null);
    }

    private void mockSuccessfulProcessing(CyclicBarrier taskCallBarrier, CyclicBarrier taskInterlockBarrier) {
        when(ctx.getProcessingState().createTask(eq(ctx.getShardConsumerArgument()), any(), any()))
                .thenReturn(ctx.getProcessingTask());
        when(ctx.getProcessingState().requiresDataAvailability()).thenReturn(true);
        when(ctx.getProcessingState().taskType()).thenReturn(TaskType.PROCESS);
        when(ctx.getProcessingTask().taskType()).thenReturn(TaskType.PROCESS);
        when(ctx.getProcessingTask().call()).thenAnswer(i -> {
            awaitBarrier(taskCallBarrier);
            awaitBarrier(taskInterlockBarrier);
            return ctx.getProcessingTaskResult();
        });
        when(ctx.getProcessingTaskResult().getException()).thenReturn(null);
        when(ctx.getProcessingState().successTransition()).thenReturn(ctx.getProcessingState());
        when(ctx.getProcessingState().shutdownTransition(any())).thenReturn(ctx.getShutdownState());
        when(ctx.getProcessingState().state()).thenReturn(ConsumerStates.ShardConsumerState.PROCESSING);
    }

    private void mockSuccessfulInitialize(CyclicBarrier taskCallBarrier) {
        mockSuccessfulInitialize(taskCallBarrier, null);
    }

    private void mockSuccessfulInitialize(CyclicBarrier taskCallBarrier, CyclicBarrier taskInterlockBarrier) {

        when(ctx.getInitialState().createTask(eq(ctx.getShardConsumerArgument()), any(), any()))
                .thenReturn(ctx.getInitializeTask());
        when(ctx.getInitialState().taskType()).thenReturn(TaskType.INITIALIZE);
        when(ctx.getInitializeTask().taskType()).thenReturn(TaskType.INITIALIZE);
        when(ctx.getInitializeTask().call()).thenAnswer(i -> {
            awaitBarrier(taskCallBarrier);
            awaitBarrier(taskInterlockBarrier);
            return ctx.getInitializeTaskResult();
        });
        when(ctx.getInitializeTaskResult().getException()).thenReturn(null);
        when(ctx.getInitialState().requiresDataAvailability()).thenReturn(false);
        when(ctx.getInitialState().successTransition()).thenReturn(ctx.getProcessingState());
        when(ctx.getInitialState().state()).thenReturn(ConsumerStates.ShardConsumerState.INITIALIZING);

    }

    private void awaitBarrier(CyclicBarrier barrier) throws Exception {
        if (barrier != null) {
            barrier.await();
        }
    }

    private void awaitAndResetBarrier(CyclicBarrier barrier) throws Exception {
        barrier.await();
        barrier.reset();
    }

    /**
     * Test to validate the warning message from ShardConsumer is not suppressed with the default configuration of 0
     * 
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterTimeoutIgnoreDefaultHappyPath() throws Exception {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = { false, false, false, false, false };
        int[] expectedLogs = { 0, 0, 0, 0, 0 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 0, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is not suppressed with the default configuration of 0
     * 
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterTimeoutIgnoreDefault() throws Exception {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = { false, false, true, false, true };
        int[] expectedLogs = { 0, 0, 1, 1, 2 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 0, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully supressed if we only have intermittant
     * readTimeouts.
     * 
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterTimeoutIgnore1() throws Exception {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = { false, false, true, false, true };
        int[] expectedLogs = { 0, 0, 0, 0, 0 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 1, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully logged if multiple sequential timeouts
     * occur.
     * 
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterMultipleTimeoutIgnore1() throws Exception {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = { true, true, false, true, true };
        int[] expectedLogs = { 0, 1, 1, 1, 2 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 1, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully logged if sequential timeouts occur.
     * This test appears to be problematic running on the build server. Running locally is fine though.
     * @throws Exception
     */
    @Test
    @Ignore
    public void testLoggingSuppressedAfterMultipleTimeoutIgnore2() throws Exception {
        Exception exceptionToThrow = new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = { true, true, true, true, true };
        int[] expectedLogs = { 0, 0, 1, 2, 3 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 2, exceptionToThrow);
    }

    /**
     * Test to validate the non-timeout warning message from ShardConsumer is not suppressed with the default
     * configuration of 0
     * 
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterExceptionDefault() throws Exception {
        // We're not throwing a ReadTimeout, so no suppression is expected.
        Exception exceptionToThrow = new RuntimeException("Uh oh Not a ReadTimeout");
        boolean[] requestsToThrowException = { false, false, true, false, true };
        int[] expectedLogs = { 0, 0, 1, 1, 2 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 0, exceptionToThrow);
    }

    /**
     * Test to validate the non-timeout warning message from ShardConsumer is not suppressed with 2 ReadTimeouts to
     * ignore
     * 
     * @throws Exception
     */
    @Test
    public void testLoggingNotSuppressedAfterExceptionIgnore2ReadTimeouts() throws Exception {
        // We're not throwing a ReadTimeout, so no suppression is expected.
        Exception exceptionToThrow = new RuntimeException("Uh oh Not a ReadTimeout");
        boolean[] requestsToThrowException = { false, false, true, false, true };
        int[] expectedLogs = { 0, 0, 1, 1, 2 };
        runLogSuppressionTest(requestsToThrowException, expectedLogs, 2, exceptionToThrow);
    }

    /**
     * Runs the log suppression test which mocks exceptions to be thrown during shard consumption and validates log
     * messages and requests recieved.
     * 
     * @param requestsToThrowException
     *            - Controls the test execution for how many requests to mock, and if they are successful, or throw an
     *            exception.
     *            true-> publish throws exception
     *            false-> publish successfully processes
     * @param expectedLogCounts
     *            - The expected warning log counts given the request profile from <tt>requestsToThrowException</tt> and
     *            <tt>readTimeoutsToIgnoreBeforeWarning</tt>
     * @param readTimeoutsToIgnoreBeforeWarning
     *            - Used to configure the ShardConsumer for the test to specify the configurable number of timeouts to
     *            suppress. This should not suppress any non-timeout exception.
     * @param exceptionToThrow
     *            - Specifies the type of exception to throw.
     * @throws Exception
     */
    private void runLogSuppressionTest(boolean[] requestsToThrowException, int[] expectedLogCounts,
            int readTimeoutsToIgnoreBeforeWarning, Exception exceptionToThrow) throws Exception {
        // Setup Test
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        mockSuccessfulInitialize(null);
        mockSuccessfulProcessing(taskCallBarrier);
        mockSuccessfulShutdown(null);
        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(true, ctx.getProcessRecordsInput(),
                requestsToThrowException, exceptionToThrow);

        // Logging supressions specific setup
        int expectedRequest = 0;
        int expectedPublish = 0;

        ShardConsumer consumer = new ShardConsumer(cache, ctx.getExecutorService(), ctx.getShardInfo(),
                ctx.getLogWarningForTaskAfterMillis(), ctx.getShardConsumerArgument(), ctx.getInitialState(),
                Function.identity(), 1, ctx.getTaskExecutionListener(), readTimeoutsToIgnoreBeforeWarning);

        Logger mockLogger = mock(Logger.class);
        injectLogger(consumer.subscriber(), mockLogger);

        // This needs to be executed in a seperate thread before an expected timeout
        // publish call to await the required cyclic barriers
        Runnable awaitingCacheThread = () -> {
            try {
                cache.awaitRequest();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        // Run the configured test
        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }
        // Initialize Shard Consumer Subscriptions
        consumer.subscribe();
        cache.awaitInitialSetup();
        for (int i = 0; i < requestsToThrowException.length; i++) {
            boolean shouldTimeout = requestsToThrowException[i];
            int expectedLogCount = expectedLogCounts[i];
            expectedRequest++;
            if (shouldTimeout) {
                // Mock a ReadTimeout call
                executor.submit(awaitingCacheThread);
                cache.publish();
                // Sleep to increase liklihood of async processing is picked up in ShardConsumer.
                // Previous cyclic barriers are used to sync Publisher with the test, this test would require another
                // subscriptionBarrier
                // in the ShardConsumer to fully sync the processing with the Test.
                Thread.sleep(50);
                // Restart subscription after failed request
                consumer.subscribe();
                cache.awaitSubscription();
            } else {
                expectedPublish++;
                // Mock a successful call
                cache.publish();
                awaitAndResetBarrierWithTimeout(taskCallBarrier);
                cache.awaitRequest();
            }
            assertEquals(expectedPublish, cache.getPublishCount());
            assertEquals(expectedRequest, cache.getRequestCount());
            if (exceptionToThrow instanceof RetryableRetrievalException
                    && exceptionToThrow.getMessage().contains("ReadTimeout")) {
                verify(mockLogger, times(expectedLogCount))
                        .warn(eq("{}: onError().  Cancelling subscription, and marking self as failed. KCL will"
                                + " recreate the subscription as neccessary to continue processing. If you "
                                + "are seeing this warning frequently consider increasing the SDK timeouts "
                                + "by providing an OverrideConfiguration to the kinesis client. Alternatively you"
                                + "can configure LifecycleConfig.readTimeoutsToIgnoreBeforeWarning to suppress"
                                + "intermittant ReadTimeout warnings."), anyString(), any());
            } else {
                verify(mockLogger, times(expectedLogCount)).warn(
                        eq("{}: onError().  Cancelling subscription, and marking self as failed. KCL will "
                                + "recreate the subscription as neccessary to continue processing."),
                        anyString(), any());
            }
        }

        // Clean Up Test
        injectLogger(consumer.subscriber(), LoggerFactory.getLogger(ShardConsumerSubscriber.class));

        Thread closingThread = new Thread(new Runnable() {
            public void run() {
                consumer.leaseLost();
            }
        });
        closingThread.start();
        cache.awaitRequest();

        // We need to await and reset the task subscriptionBarrier prior to going into the shutdown loop.
        Runnable awaitingTaskThread = () -> {
            try {
                awaitAndResetBarrierWithTimeout(taskCallBarrier);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        executor.submit(awaitingTaskThread);
        while (!consumer.shutdownComplete().get()) {
        }
    }

    /**
     * Use reflection to inject a logger for verification. This will mute any logging occuring with this logger,
     * but allow it to be verifiable.
     *
     * After executing the test, a normal Logger from a standard LoggerFactory should be injected to continue logging
     * as expected.
     */
    private void injectLogger(final ShardConsumerSubscriber subscriber, final Logger logger) throws SecurityException,
            NoSuchFieldException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException {
        // Get the private field
        final Field field = subscriber.getClass().getDeclaredField("log");
        // Allow modification on the field
        field.setAccessible(true);
        // Make the logger non-final
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        // Inject the mock...
        field.set(subscriber, logger);

    }

    public static void awaitBarrierWithTimeout(CyclicBarrier barrier) throws Exception {
        if (barrier != null) {
            barrier.await(ShardConsumerTestContext.awaitTimeout, ShardConsumerTestContext.awaitTimeoutUnit);
        }
    }

    public static void awaitAndResetBarrierWithTimeout(CyclicBarrier barrier) throws Exception {
        if (barrier != null) {
            barrier.await(ShardConsumerTestContext.awaitTimeout, ShardConsumerTestContext.awaitTimeoutUnit);
            barrier.reset();
        }
    }

    /**
     * Test Publisher with seperate barriers for Task processing and creating a subscription.
     *
     * These barriers need to be "primed" in a seperate thread via the await/reset methods to allow processing
     * to sync along these barriers.
     *
     * Optionally, you can errors also trigger the Task Processing Barrier if you are wanting to validate
     * handling around error conditions.
     */
    public class CyclicBarrierTestPublisher implements RecordsPublisher {
        protected final CyclicBarrier subscriptionBarrier = new CyclicBarrier(2);
        protected final CyclicBarrier requestBarrier = new CyclicBarrier(2);
        private final Exception exceptionToThrow;
        private final boolean[] requestsToThrowException;

        public int getRequestCount() {
            return requestCount.get();
        }

        public int getPublishCount() {
            return publishCount;
        }

        private AtomicInteger requestCount = new AtomicInteger(0);

        Subscriber<? super RecordsRetrieved> subscriber;
        final Subscription subscription = mock(Subscription.class);
        private int publishCount = 0;
        private ProcessRecordsInput processRecordsInput;

        CyclicBarrierTestPublisher(ProcessRecordsInput processRecordsInput) {
            this(false, processRecordsInput);
        }

        CyclicBarrierTestPublisher(boolean enableCancelAwait, ProcessRecordsInput processRecordsInput) {
            this(enableCancelAwait, processRecordsInput, null, null);
        }

        CyclicBarrierTestPublisher(boolean enableCancelAwait, ProcessRecordsInput processRecordsInput,
                boolean[] requestsToThrowException, Exception exceptionToThrow) {
            doAnswer(a -> {
                requestBarrier.await(ShardConsumerTestContext.awaitTimeout, ShardConsumerTestContext.awaitTimeoutUnit);
                return null;
            }).when(subscription).request(anyLong());
            doAnswer(a -> {
                if (enableCancelAwait) {
                    requestBarrier.await(ShardConsumerTestContext.awaitTimeout,
                            ShardConsumerTestContext.awaitTimeoutUnit);
                }
                return null;
            }).when(subscription).cancel();
            this.requestsToThrowException = requestsToThrowException;
            this.exceptionToThrow = exceptionToThrow;
            this.processRecordsInput = processRecordsInput;
        }

        @Override
        public void start(ExtendedSequenceNumber extendedSequenceNumber,
                InitialPositionInStreamExtended initialPositionInStreamExtended) {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public void subscribe(Subscriber<? super RecordsRetrieved> s) {
            subscriber = s;
            subscriber.onSubscribe(subscription);
            try {
                subscriptionBarrier.await(ShardConsumerTestContext.awaitTimeout,
                        ShardConsumerTestContext.awaitTimeoutUnit);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void restartFrom(RecordsRetrieved recordsRetrieved) {

        }

        public void awaitSubscription() throws InterruptedException, BrokenBarrierException, TimeoutException {
            subscriptionBarrier.await(ShardConsumerTestContext.awaitTimeout, ShardConsumerTestContext.awaitTimeoutUnit);
            subscriptionBarrier.reset();
        }

        public void awaitRequest() throws InterruptedException, BrokenBarrierException, TimeoutException {
            requestBarrier.await(ShardConsumerTestContext.awaitTimeout, ShardConsumerTestContext.awaitTimeoutUnit);
            requestBarrier.reset();
        }

        public void awaitInitialSetup() throws InterruptedException, BrokenBarrierException, TimeoutException {
            awaitRequest();
            awaitSubscription();
        }

        public void publish() {
            if (requestsToThrowException != null
                    && requestsToThrowException[requestCount.getAndIncrement() % requestsToThrowException.length]) {
                subscriber.onError(exceptionToThrow);
            } else {
                publish(() -> processRecordsInput);
            }
        }

        public void publish(RecordsRetrieved input) {
            subscriber.onNext(input);
            publishCount++;
        }
    }

}
