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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.RequestDetails;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.ConsumerStates.ShardConsumerState;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput;
import software.amazon.kinesis.retrieval.RecordsDeliveryAck;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests of {@link ShardConsumer}.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class ShardConsumerTest {

    private final String shardId = "shardId-0-0";
    private final String concurrencyToken = "TestToken";
    private final RequestDetails lastSuccessfulRequestDetails = new RequestDetails();
    private ShardInfo shardInfo;
    private TaskExecutionListenerInput initialTaskInput;
    private TaskExecutionListenerInput processTaskInput;
    private TaskExecutionListenerInput shutdownTaskInput;
    private TaskExecutionListenerInput shutdownRequestedTaskInput;
    private TaskExecutionListenerInput shutdownRequestedAwaitTaskInput;

    private ExecutorService executorService;

    @Mock
    private RecordsPublisher recordsPublisher;

    @Mock
    private ShutdownNotification shutdownNotification;

    @Mock
    private ConsumerState blockedOnParentsState;

    @Mock
    private ConsumerTask blockedOnParentsTask;

    @Mock
    private ConsumerState initialState;

    @Mock
    private ConsumerTask initializeTask;

    @Mock
    private ConsumerState processingState;

    @Mock
    private ConsumerTask processingTask;

    @Mock
    private ConsumerState shutdownState;

    @Mock
    private ConsumerTask shutdownTask;

    @Mock
    private TaskResult initializeTaskResult;

    @Mock
    private TaskResult processingTaskResult;

    @Mock
    private TaskResult blockOnParentsTaskResult;

    @Mock
    private ConsumerState shutdownCompleteState;

    @Mock
    private ShardConsumerArgument shardConsumerArgument;

    @Mock
    private ConsumerState shutdownRequestedState;

    @Mock
    private ConsumerTask shutdownRequestedTask;

    @Mock
    private ConsumerState shutdownRequestedAwaitState;

    @Mock
    private TaskExecutionListener taskExecutionListener;

    private ProcessRecordsInput processRecordsInput;

    private Optional<Long> logWarningForTaskAfterMillis = Optional.empty();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        shardInfo = new ShardInfo(shardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("test-" + testName.getMethodName() + "-%04d")
                .setDaemon(true)
                .build();
        executorService = new ThreadPoolExecutor(4, 4, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), factory);

        processRecordsInput = ProcessRecordsInput.builder()
                .isAtShardEnd(false)
                .cacheEntryTime(Instant.now())
                .millisBehindLatest(1000L)
                .records(Collections.emptyList())
                .build();
        initialTaskInput = TaskExecutionListenerInput.builder()
                .shardInfo(shardInfo)
                .taskType(TaskType.INITIALIZE)
                .build();
        processTaskInput = TaskExecutionListenerInput.builder()
                .shardInfo(shardInfo)
                .taskType(TaskType.PROCESS)
                .build();
        shutdownRequestedTaskInput = TaskExecutionListenerInput.builder()
                .shardInfo(shardInfo)
                .taskType(TaskType.SHUTDOWN_NOTIFICATION)
                .build();
        shutdownRequestedAwaitTaskInput = TaskExecutionListenerInput.builder()
                .shardInfo(shardInfo)
                .taskType(TaskType.SHUTDOWN_COMPLETE)
                .build();
        shutdownTaskInput = TaskExecutionListenerInput.builder()
                .shardInfo(shardInfo)
                .taskType(TaskType.SHUTDOWN)
                .build();
    }

    @After
    public void after() {
        List<Runnable> remainder = executorService.shutdownNow();
        assertTrue(remainder.isEmpty());
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
                    })
                    .when(subscription)
                    .request(anyLong());
            doAnswer(a -> {
                        if (enableCancelAwait) {
                            requestBarrier.await();
                        }
                        return null;
                    })
                    .when(subscription)
                    .cancel();
        }

        @Override
        public void start(
                ExtendedSequenceNumber extendedSequenceNumber,
                InitialPositionInStreamExtended initialPositionInStreamExtended) {}

        @Override
        public void notify(RecordsDeliveryAck ack) {}

        @Override
        public void shutdown() {}

        @Override
        public RequestDetails getLastSuccessfulRequestDetails() {
            return lastSuccessfulRequestDetails;
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
        public void restartFrom(RecordsRetrieved recordsRetrieved) {}

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
            publish(() -> processRecordsInput);
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
        final ShardConsumer consumer = createShardConsumer(cache);

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
        verify(processingState, times(2)).createTask(eq(shardConsumerArgument), eq(consumer), any(), any());
        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).beforeTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownTaskInput);

        initialTaskInput =
                initialTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        processTaskInput =
                processTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        shutdownTaskInput = shutdownTaskInput.toBuilder()
                .taskOutcome(TaskOutcome.SUCCESSFUL)
                .build();

        verify(taskExecutionListener, times(1)).afterTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).afterTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownTaskInput);
        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test(timeout = 1000L)
    public void testLeaseLossIsNonBlocking() throws Exception {
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);
        CyclicBarrier processingTaskInterlock = new CyclicBarrier(2);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskCallBarrier, processingTaskInterlock);

        mockSuccessfulShutdown(null);

        TestPublisher cache = new TestPublisher();
        final ShardConsumer consumer = createShardConsumer(cache);

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
        assertFalse(consumer.isShutdown());

        log.debug("Release processing task interlock");
        awaitAndResetBarrier(processingTaskInterlock);

        while (!consumer.isShutdown()) {
            consumer.executeLifecycle();
            Thread.yield();
        }

        verify(cache.subscription, times(1)).request(anyLong());
        verify(cache.subscription).cancel();
        verify(processingState, times(1)).createTask(eq(shardConsumerArgument), eq(consumer), any(), any());
        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownTaskInput);

        initialTaskInput =
                initialTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        processTaskInput =
                processTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        shutdownTaskInput = shutdownTaskInput.toBuilder()
                .taskOutcome(TaskOutcome.SUCCESSFUL)
                .build();

        verify(taskExecutionListener, times(1)).afterTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownTaskInput);
        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    public void testDataArrivesAfterProcessing2() throws Exception {
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskCallBarrier);

        mockSuccessfulShutdown(null);

        TestPublisher cache = new TestPublisher();
        final ShardConsumer consumer = createShardConsumer(cache);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);

        verify(processingState).createTask(any(), any(), any(), any());
        verify(processingTask).call();

        cache.awaitRequest();

        cache.publish();
        awaitAndResetBarrier(taskCallBarrier);
        verify(processingState, times(2)).createTask(any(), any(), any(), any());
        verify(processingTask, times(2)).call();

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

        verify(processingState, times(3)).createTask(any(), any(), any(), any());
        verify(processingTask, times(3)).call();
        verify(processingState).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(shutdownState).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(3)).beforeTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownTaskInput);

        initialTaskInput =
                initialTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        processTaskInput =
                processTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        shutdownTaskInput = shutdownTaskInput.toBuilder()
                .taskOutcome(TaskOutcome.SUCCESSFUL)
                .build();

        verify(taskExecutionListener, times(1)).afterTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(3)).afterTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownTaskInput);
        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    @Ignore
    public final void testInitializationStateUponFailure() throws Exception {
        final ShardConsumer consumer = createShardConsumer(recordsPublisher);

        when(initialState.createTask(eq(shardConsumerArgument), eq(consumer), any(), any()))
                .thenReturn(initializeTask);
        when(initializeTask.call()).thenReturn(new TaskResult(new Exception("Bad")));
        when(initializeTask.taskType()).thenReturn(TaskType.INITIALIZE);
        when(initialState.failureTransition()).thenReturn(initialState);

        CyclicBarrier taskBarrier = new CyclicBarrier(2);

        when(initialState.requiresDataAvailability()).thenAnswer(i -> {
            taskBarrier.await();
            return false;
        });

        consumer.executeLifecycle();
        for (int i = 0; i < 4; ++i) {
            awaitAndResetBarrier(taskBarrier);
        }

        verify(initialState, times(5)).createTask(eq(shardConsumerArgument), eq(consumer), any(), any());
        verify(initialState, never()).successTransition();
        verify(initialState, never()).shutdownTransition(any());
    }

    /**
     * Test method to verify consumer undergoes the transition WAITING_ON_PARENT_SHARDS -> INITIALIZING -> PROCESSING
     */
    @Test
    public final void testSuccessfulConsumerStateTransition() {
        ExecutorService directExecutorService = spy(executorService);

        doAnswer(this::directlyExecuteRunnable).when(directExecutorService).execute(any());

        final ShardConsumer consumer = createShardConsumer(directExecutorService, blockedOnParentsState);

        mockSuccessfulUnblockOnParents();
        mockSuccessfulInitializeWithFailureTransition();
        mockSuccessfulProcessing(null);

        int arbitraryExecutionCount = 3;
        do {
            try {
                consumer.executeLifecycle();
            } catch (Exception e) {
                // Suppress any exception like the scheduler.
                fail("Unexpected exception while executing consumer lifecycle");
            }
        } while (--arbitraryExecutionCount > 0);

        assertEquals(
                ShardConsumerState.PROCESSING.consumerState().state(),
                consumer.currentState().state());
        verify(directExecutorService, times(2)).execute(any());
    }

    /**
     * Test method to verify consumer does not transition to PROCESSING from WAITING_ON_PARENT_SHARDS when
     * INITIALIZING tasks gets rejected.
     */
    @Test
    public final void testConsumerNotTransitionsToProcessingWhenInitializationFails() {
        ExecutorService failingService = spy(executorService);
        final ShardConsumer consumer = createShardConsumer(failingService, blockedOnParentsState);

        mockSuccessfulUnblockOnParents();
        mockSuccessfulInitializeWithFailureTransition();
        mockSuccessfulProcessing(null);

        // Failing the initialization task and all other attempts after that.
        doAnswer(this::directlyExecuteRunnable)
                .doThrow(new RejectedExecutionException())
                .when(failingService)
                .execute(any());

        int arbitraryExecutionCount = 5;
        do {
            try {
                consumer.executeLifecycle();
            } catch (Exception e) {
                // Suppress any exception like the scheduler.
                fail("Unexpected exception while executing consumer lifecycle");
            }
        } while (--arbitraryExecutionCount > 0);

        assertEquals(
                ShardConsumerState.INITIALIZING.consumerState().state(),
                consumer.currentState().state());
        verify(failingService, times(5)).execute(any());
    }

    /**
     * Test method to verify consumer transition to PROCESSING from WAITING_ON_PARENT_SHARDS with
     * intermittent INITIALIZING task rejections.
     */
    @Test
    public final void testConsumerTransitionsToProcessingWithIntermittentInitializationFailures() {
        ExecutorService failingService = spy(executorService);
        final ShardConsumer consumer = createShardConsumer(failingService, blockedOnParentsState);

        mockSuccessfulUnblockOnParents();
        mockSuccessfulInitializeWithFailureTransition();
        mockSuccessfulProcessing(null);

        // Failing the initialization task and few other attempts after that.
        doAnswer(this::directlyExecuteRunnable)
                .doThrow(new RejectedExecutionException())
                .doThrow(new RejectedExecutionException())
                .doThrow(new RejectedExecutionException())
                .doAnswer(this::directlyExecuteRunnable)
                .when(failingService)
                .execute(any());

        int arbitraryExecutionCount = 6;
        do {
            try {
                consumer.executeLifecycle();
            } catch (Exception e) {
                // Suppress any exception like the scheduler.
                fail("Unexpected exception while executing consumer lifecycle");
            }
        } while (--arbitraryExecutionCount > 0);

        assertEquals(
                ShardConsumerState.PROCESSING.consumerState().state(),
                consumer.currentState().state());
        verify(failingService, times(5)).execute(any());
    }

    /**
     * Test method to verify consumer does not transition to INITIALIZING when WAITING_ON_PARENT_SHARDS task rejected.
     */
    @Test
    public final void testConsumerNotTransitionsToInitializingWhenWaitingOnParentsFails() {
        ExecutorService failingService = spy(executorService);
        final ShardConsumer consumer = createShardConsumer(failingService, blockedOnParentsState);

        mockSuccessfulUnblockOnParentsWithFailureTransition();
        mockSuccessfulInitializeWithFailureTransition();

        // Failing the waiting_on_parents task and few other attempts after that.
        doThrow(new RejectedExecutionException()).when(failingService).execute(any());

        int arbitraryExecutionCount = 5;
        do {
            try {
                consumer.executeLifecycle();
            } catch (Exception e) {
                // Suppress any exception like the scheduler.
                fail("Unexpected exception while executing consumer lifecycle");
            }
        } while (--arbitraryExecutionCount > 0);

        assertEquals(
                ShardConsumerState.WAITING_ON_PARENT_SHARDS.consumerState().state(),
                consumer.currentState().state());
        verify(failingService, times(5)).execute(any());
    }

    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @Test(expected = RejectedExecutionException.class)
    public final void testInitializationStateUponSubmissionFailure() throws Exception {
        ExecutorService failingService = mock(ExecutorService.class);
        final ShardConsumer consumer = createShardConsumer(failingService, initialState);

        doThrow(new RejectedExecutionException()).when(failingService).execute(any());

        boolean initComplete;
        do {
            initComplete = consumer.initializeComplete().get();
        } while (!initComplete);
        verifyZeroInteractions(taskExecutionListener);
    }

    @Test
    public void testErrorThrowableInInitialization() throws Exception {
        final ShardConsumer consumer = createShardConsumer(recordsPublisher);

        when(initialState.createTask(any(), any(), any(), any())).thenReturn(initializeTask);
        when(initialState.taskType()).thenReturn(TaskType.INITIALIZE);
        when(initializeTask.call()).thenAnswer(i -> {
            throw new Error("Error");
        });

        try {
            consumer.initializeComplete().get();
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(Error.class));
        }
        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    public void testRequestedShutdownWhileQuiet() throws Exception {
        CyclicBarrier taskBarrier = new CyclicBarrier(2);

        TestPublisher cache = new TestPublisher();
        final ShardConsumer consumer = createShardConsumer(cache);

        mockSuccessfulInitialize(null);

        mockSuccessfulProcessing(taskBarrier);

        when(processingState.shutdownTransition(eq(ShutdownReason.REQUESTED))).thenReturn(shutdownRequestedState);
        when(shutdownRequestedState.createTask(any(), any(), any(), any())).thenReturn(shutdownRequestedTask);
        when(shutdownRequestedState.taskType()).thenReturn(TaskType.SHUTDOWN_NOTIFICATION);
        when(shutdownRequestedTask.call()).thenReturn(new TaskResult(null));

        when(shutdownRequestedState.shutdownTransition(eq(ShutdownReason.REQUESTED)))
                .thenReturn(shutdownRequestedAwaitState);
        when(shutdownRequestedAwaitState.createTask(any(), any(), any(), any())).thenReturn(null);
        when(shutdownRequestedAwaitState.shutdownTransition(eq(ShutdownReason.LEASE_LOST)))
                .thenReturn(shutdownState);
        when(shutdownRequestedAwaitState.taskType()).thenReturn(TaskType.SHUTDOWN_COMPLETE);

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

        consumer.gracefulShutdown(shutdownNotification);
        boolean shutdownComplete = consumer.shutdownComplete().get();
        assertFalse(shutdownComplete);
        shutdownComplete = consumer.shutdownComplete().get();
        assertFalse(shutdownComplete);

        consumer.leaseLost();
        shutdownComplete = consumer.shutdownComplete().get();
        assertFalse(shutdownComplete);
        shutdownComplete = consumer.shutdownComplete().get();
        assertTrue(shutdownComplete);

        verify(processingState, times(2)).createTask(any(), any(), any(), any());
        verify(shutdownRequestedState, never()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(shutdownRequestedState).createTask(any(), any(), any(), any());
        verify(shutdownRequestedState).shutdownTransition(eq(ShutdownReason.REQUESTED));
        verify(shutdownRequestedAwaitState).createTask(any(), any(), any(), any());
        verify(shutdownRequestedAwaitState).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).beforeTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownRequestedTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownRequestedAwaitTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownTaskInput);

        initialTaskInput =
                initialTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        processTaskInput =
                processTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        shutdownRequestedTaskInput = shutdownRequestedTaskInput.toBuilder()
                .taskOutcome(TaskOutcome.SUCCESSFUL)
                .build();
        shutdownTaskInput = shutdownTaskInput.toBuilder()
                .taskOutcome(TaskOutcome.SUCCESSFUL)
                .build();
        // No task is created/run for this shutdownRequestedAwaitState, so there's no task outcome.

        // shutdownNotification.shutdownComplete() should only be called for gracefulShutdown
        verify(shutdownNotification, times(1)).shutdownComplete();

        verify(taskExecutionListener, times(1)).afterTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).afterTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownRequestedTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownRequestedAwaitTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownTaskInput);
        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    public void testExceptionInProcessingRetriesSameBatch() throws Exception {
        TestPublisher cache = new TestPublisher();

        ShardConsumer consumer = new ShardConsumer(
                cache,
                executorService,
                shardInfo,
                Optional.of(1L),
                shardConsumerArgument,
                initialState,
                1,
                taskExecutionListener,
                0,
                new KinesisConsumerTaskFactory());
        consumer.subscriber().dispatchRetryInitialBackoffMillis = 1L;

        mockSuccessfulInitialize(null);
        mockSuccessfulProcessing(null);

        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);
        final RuntimeException expectedException = new RuntimeException("Whee");
        final AtomicInteger calls = new AtomicInteger();
        when(processingTask.call()).thenAnswer(a -> {
            try {
                if (calls.incrementAndGet() == 1) {
                    throw expectedException;
                }
                return processingTaskResult;
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
        // First attempt fails, the same batch is retried and succeeds.
        awaitAndResetBarrier(taskCallBarrier);
        awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        // The failure is retried, not skipped, so nothing is surfaced by healthCheck.
        assertThat(consumer.healthCheck(), nullValue());

        verify(processingTask, times(2)).call();
        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).beforeTaskExecution(processTaskInput);
    }

    @Test
    public void testLongRunningTasks() throws Exception {
        TestPublisher cache = new TestPublisher();

        ShardConsumer consumer = new ShardConsumer(
                cache,
                executorService,
                shardInfo,
                Optional.of(1L),
                shardConsumerArgument,
                initialState,
                1,
                taskExecutionListener,
                0,
                new KinesisConsumerTaskFactory());

        CyclicBarrier taskArriveBarrier = new CyclicBarrier(2);
        CyclicBarrier taskDepartBarrier = new CyclicBarrier(2);

        mockSuccessfulInitialize(taskArriveBarrier, taskDepartBarrier);
        mockSuccessfulProcessing(taskArriveBarrier, taskDepartBarrier);
        mockSuccessfulShutdown(taskArriveBarrier, taskDepartBarrier);

        CompletableFuture<Boolean> initSuccess = consumer.initializeComplete();

        awaitAndResetBarrier(taskArriveBarrier);
        assertNotNull(consumer.taskRunningTime());
        consumer.healthCheck();
        awaitAndResetBarrier(taskDepartBarrier);

        assertFalse(initSuccess.get());
        verify(initializeTask).call();

        initSuccess = consumer.initializeComplete();
        verify(initializeTask).call();
        assertTrue(initSuccess.get());
        consumer.healthCheck();

        assertNull(consumer.taskRunningTime());

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();

        awaitAndResetBarrier(taskArriveBarrier);
        Instant previousTaskStartTime = consumer.taskDispatchedAt();
        assertNotNull(consumer.taskRunningTime());
        consumer.healthCheck();
        awaitAndResetBarrier(taskDepartBarrier);

        consumer.healthCheck();

        cache.requestBarrier.await();
        assertNull(consumer.taskRunningTime());
        cache.requestBarrier.reset();

        // Sleep for 10 millis before processing next task. If we don't; then the following
        // assertion on time fails. This happens if cache.publish() is executed in the
        // same millisecond as that of previousTaskStartTime, resulting in the unit test failure.
        Thread.sleep(10);
        cache.publish();

        awaitAndResetBarrier(taskArriveBarrier);
        Instant currentTaskStartTime = consumer.taskDispatchedAt();
        assertNotEquals(currentTaskStartTime, previousTaskStartTime);
        awaitAndResetBarrier(taskDepartBarrier);

        cache.requestBarrier.await();
        assertNull(consumer.taskRunningTime());
        cache.requestBarrier.reset();

        consumer.leaseLost();

        assertTrue(consumer.isShutdownRequested());
        CompletableFuture<Boolean> shutdownComplete = consumer.shutdownComplete();

        awaitAndResetBarrier(taskArriveBarrier);
        assertNotNull(consumer.taskRunningTime());
        awaitAndResetBarrier(taskDepartBarrier);

        assertFalse(shutdownComplete.get());

        shutdownComplete = consumer.shutdownComplete();
        assertTrue(shutdownComplete.get());

        assertNull(consumer.taskRunningTime());
        consumer.healthCheck();

        verify(taskExecutionListener, times(1)).beforeTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).beforeTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).beforeTaskExecution(shutdownTaskInput);

        initialTaskInput =
                initialTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        processTaskInput =
                processTaskInput.toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build();
        shutdownTaskInput = shutdownTaskInput.toBuilder()
                .taskOutcome(TaskOutcome.SUCCESSFUL)
                .build();

        verify(taskExecutionListener, times(1)).afterTaskExecution(initialTaskInput);
        verify(taskExecutionListener, times(2)).afterTaskExecution(processTaskInput);
        verify(taskExecutionListener, times(1)).afterTaskExecution(shutdownTaskInput);
        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    public void testEmptyShardProcessingRaceCondition() throws Exception {
        final RecordsPublisher mockPublisher = mock(RecordsPublisher.class);
        final ExecutorService mockExecutor = mock(ExecutorService.class);
        final ConsumerState mockState = mock(ConsumerState.class);
        final ShardConsumer consumer = new ShardConsumer(
                mockPublisher,
                mockExecutor,
                shardInfo,
                Optional.of(1L),
                shardConsumerArgument,
                mockState,
                1,
                taskExecutionListener,
                0,
                new KinesisConsumerTaskFactory());

        when(mockState.state()).thenReturn(ShardConsumerState.WAITING_ON_PARENT_SHARDS);
        when(mockState.taskType()).thenReturn(TaskType.BLOCK_ON_PARENT_SHARDS);
        final ConsumerTask mockTask = mock(ConsumerTask.class);
        when(mockState.createTask(any(), any(), any(), any())).thenReturn(mockTask);
        // Simulate successful BlockedOnParent task execution
        // and successful Initialize task execution
        when(mockTask.call()).thenReturn(new TaskResult(false));

        log.info("Scheduler Thread: Invoking ShardConsumer.executeLifecycle() to initiate async"
                + " processing of blocked on parent task");
        consumer.executeLifecycle();
        final ArgumentCaptor<Runnable> taskToExecute = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor, timeout(100)).execute(taskToExecute.capture());
        taskToExecute.getValue().run();
        log.info("RecordProcessor Thread: Simulated successful execution of Blocked on parent task");
        reset(mockExecutor);

        log.info("Scheduler Thread: Invoking ShardConsumer.executeLifecycle() to move to InitializingState"
                + " and initiate async processing of initialize task");
        when(mockState.successTransition()).thenReturn(mockState);
        when(mockState.state()).thenReturn(ShardConsumerState.INITIALIZING);
        when(mockState.taskType()).thenReturn(TaskType.INITIALIZE);
        consumer.executeLifecycle();
        verify(mockExecutor, timeout(100)).execute(taskToExecute.capture());
        log.info("RecordProcessor Thread: Simulated successful execution of Initialize task");
        taskToExecute.getValue().run();

        log.info("Scheduler Thread: Invoking ShardConsumer.executeLifecycle() to move to ProcessingState"
                + " and mark initialization future as complete");
        when(mockState.state()).thenReturn(ShardConsumerState.PROCESSING);
        consumer.executeLifecycle();

        // Simulate the race where
        // scheduler invokes executeLifecycle which performs Publisher.subscribe(subscriber)
        // on recordProcessor thread
        // but before scheduler thread finishes initialization, handleInput is invoked
        // on record processor thread.

        // Since ShardConsumer creates its own instance of subscriber that cannot be mocked
        // this test sequence will appear a little odd.
        // In order to control the order in which execution occurs, lets first invoke
        // handleInput, although this will never happen, since there isn't a way
        // to control the precise timing of the thread execution, this is the best way
        final CountDownLatch processTaskLatch = new CountDownLatch(1);
        new Thread(() -> {
                    reset(mockState);
                    when(mockState.taskType()).thenReturn(TaskType.PROCESS);
                    final ConsumerTask mockProcessTask = mock(ConsumerTask.class);
                    when(mockState.createTask(any(), any(), any(), any())).thenReturn(mockProcessTask);
                    when(mockProcessTask.call()).then(input -> {
                        // first we want to wait for subscribe to be called,
                        // but we cannot control the timing, so wait for 10 seconds
                        // to let the main thread invoke executeLifecyle which
                        // will perform subscribe
                        processTaskLatch.countDown();
                        log.info("Record Processor Thread: Holding shardConsumer lock, waiting for 10 seconds to"
                                + " let subscribe be called by scheduler thread");
                        Thread.sleep(10 * 1000);
                        log.info("RecordProcessor Thread: Done waiting");
                        // then return shard end result
                        log.info(
                                "RecordProcessor Thread: Simulating execution of ProcessTask and returning shard-end result");
                        return new TaskResult(true);
                    });
                    final Subscription mockSubscription = mock(Subscription.class);
                    consumer.handleInput(
                            ProcessRecordsInput.builder().isAtShardEnd(true).build(), mockSubscription);
                })
                .start();

        processTaskLatch.await();

        // invoke executeLifecycle, which should invoke subscribe
        // meanwhile if scheduler tries to acquire the ShardConsumer lock it will
        // be blocked during initialization processing because handleInput was
        // already invoked and will be holding the lock. Thereby creating the
        // race condition we want.
        reset(mockState);
        AtomicBoolean successTransitionCalled = new AtomicBoolean(false);
        AtomicBoolean shutdownTransitionCalled = new AtomicBoolean(false);
        when(mockState.state()).then(input -> {
            if (successTransitionCalled.get() && shutdownTransitionCalled.get()) {
                return ShardConsumerState.SHUTTING_DOWN;
            }
            return ShardConsumerState.PROCESSING;
        });
        log.info("Scheduler Thread: Invoking ShardConsumer.executeLifecycle() to invoke subscribe and"
                + " complete initialization");
        consumer.executeLifecycle();
        log.info("Scheduler Thread: Done initializing the ShardConsumer");

        log.info("Verifying scheduler did not perform shutdown transition during initialization");
        verify(mockState, times(0)).shutdownTransition(any());
    }

    // =====================================================================================================
    // SHARD_END shutdown racing with a lease loss (leader-side SHARD_END cleanup)
    //
    // The leader evicts (or deletes) a SHARD_END lease once its SHARD_END checkpoint is in the lease table, which
    // can be while this worker's SHARD_END shutdown task is still running. These tests check which shutdown tasks
    // the ShardConsumer creates, and with which shutdown reason, when the lease is lost during that task. What each
    // task then does is checked in ShutdownTaskTest.
    //
    // If the SHARD_END shutdown task fails (for example, shardEnded() threw), the consumer retries it. The retry's
    // reason depends only on whether the Scheduler has called ShardConsumer.leaseLost() by the time the retry is
    // created; what the retry then does depends on whether the lease is held and whether shardEnded() checkpointed:
    //
    //  #  Lease at retry                    Checkpointed?  Retry reason  What the retry's ShutdownTask does
    //  -  --------------------------------  -------------  ------------  ---------------------------------------
    //  1  Still held                        yes            SHARD_END     Existing path: skips shardEnded(),
    //                                                                    queues the lease for cleanup.
    //  2  Still held                        no             SHARD_END     Existing path: calls shardEnded() again.
    //  3  Lost, Scheduler not yet notified  yes            SHARD_END     New check: no further application calls.
    //  4  Lost, Scheduler notified          yes            LEASE_LOST    New check: no further application calls.
    //  5  Lost, Scheduler not yet notified  no             SHARD_END     leaseLost(): a real lease loss.
    //  6  Lost, Scheduler notified          no             LEASE_LOST    leaseLost(): a real lease loss.
    //
    // "Existing path" is ShutdownTask's SHARD_END path, unchanged by this change: it reads the lease from the lease
    // table, and skips shardEnded() if it's already at SHARD_END. "New check" is the check this change added at the
    // top of ShutdownTask.call(): if the lease isn't held and this processor's shardEnded() already checkpointed
    // SHARD_END, the task finishes without calling the record processor.
    //
    // How these tests drive the consumer: once shutdown is requested, the Scheduler's executeLifecycle() call only
    // starts ShardConsumer.shutdownComplete() on the consumer's executor thread. Each shutdownComplete() call is one
    // step: it applies the previous task's outcome (moving the consumer to a new state, or not), then, if the
    // consumer isn't shut down, creates and runs the next shutdown task, and its future returns whether the consumer
    // is shut down. These tests call shutdownComplete() directly and wait on each future, so every step is explicit
    // and they assert exactly how many steps each case takes. (Through executeLifecycle(), the test would have to
    // poll, because executeLifecycle() returns right away and does nothing while the previous step is running.)
    //
    // The Scheduler's other part is telling the consumer its lease is lost: once the renewer drops the lease, the
    // Scheduler's next pass finds the shard unassigned and cleanupShardConsumers calls ShardConsumer.leaseLost(),
    // which raises the consumer's shutdown reason to LEASE_LOST. A test that loses the lease mid-task calls
    // leaseLost() once, in its place.
    //
    // Which tests cover each row:
    //  - Retry reason, in this section:
    //     - SHARD_END retry (rows 1-3 and 5):
    //       testFailedShardEndShutdown_schedulerHasNotSeenLeaseLossBeforeRetry_retryIsShardEndShutdownTask. The
    // consumer
    //       can't tell these rows apart; only the ShutdownTask checks whether the lease is held.
    //     - LEASE_LOST retry (rows 4 and 6):
    //       testFailedShardEndShutdown_schedulerSawLeaseLossBeforeRetry_retryIsLeaseLostShutdownTask.
    //     - No retry at all, when the task succeeds even though the lease was lost while it ran:
    //       testLeaseLostDuringShardEndShutdown_doesNotStartSecondShutdownTask.
    //  - What the retry does, in ShutdownTaskTest:
    //     - Row 1: testShardEndRetryAfterShardEndedThrowsAndLeaseStillHeld_noFurtherApplicationCalls.
    //     - Row 2: the existing retry behavior, not changed and not covered by a new test.
    //     - Row 3: testShardEndRetryAfterShardEndedThrowsAndLeaseEvicted_noFurtherApplicationCalls.
    //     - Row 4: testLeaseLostRetryAfterShardEndedThrowsAndLeaseEvicted_noFurtherApplicationCalls.
    //     - Row 5: testLeaseLostAfterShardEndedThrewBeforeCheckpoint_shardEndRetry_callsLeaseLostViaNotOwnedPath.
    //     - Row 6: testLeaseLostAfterShardEndedThrewBeforeCheckpoint_leaseLostRetry_callsLeaseLost.
    // =====================================================================================================

    /**
     * The leader can evict a SHARD_END lease while the worker's ShutdownTask is still running, for example while the
     * record processor's shardEnded() does work after its SHARD_END checkpoint. This worker's next lease renewal then
     * fails, and the Scheduler calls {@link ShardConsumer#leaseLost()} ({@code Scheduler.cleanupShardConsumers}).
     *
     * <p>Two different methods are named leaseLost(). {@code ShardConsumer.leaseLost()}, which the Scheduler calls
     * and this test calls in its place, only raises the consumer's shutdown reason (a field on the ShardConsumer)
     * to LEASE_LOST. It doesn't run anything. The record processor's {@code leaseLost()} callback is only called by a
     * ShutdownTask created with reason LEASE_LOST. Each ShutdownTask copies the consumer's reason when it's
     * created, so the task that's already running keeps SHARD_END.
     *
     * <p>This checks that raising the reason during a successful SHARD_END shutdown task doesn't lead to a second
     * shutdown task. In the SHUTTING_DOWN state, a successful task moves the consumer to SHUTDOWN_COMPLETE, and the
     * shutdown transition for the raised reason is SHUTDOWN_COMPLETE too, so the consumer is done and creates no
     * further task. With no LEASE_LOST task, nothing calls the record processor's leaseLost(). (If the task failed
     * instead, the consumer would stay in SHUTTING_DOWN and create a retry with the raised reason; see the next
     * test.)
     *
     * <p>When this happens in practice: only when the lease is lost after the SHARD_END checkpoint is already in
     * the lease table. A loss before the checkpoint makes the record processor's checkpoint() fail (the checkpointer
     * checks that the lease is held, and its write is conditional on the lease counter), so the task fails instead;
     * that's the next test. So the task succeeds despite the loss in one of two ways:
     * <ul>
     *   <li>shardEnded() checkpointed SHARD_END, and the lease was lost while it did later work. Leader eviction can
     *       only cause this case, because the leader waits for SHARD_END in the lease table; lease expiry or a
     *       rebalance can too.</li>
     *   <li>The lease was already at SHARD_END when the task started (for example, this task is itself a retry),
     *       so the task skips shardEnded() and needs no checkpoint.</li>
     * </ul>
     *
     * <p>Compared with the other tests in this section: this is the only one where the first task succeeds, so no
     * retry is created. The next two make the first task fail and check the retry's reason, with and without a lease
     * loss.
     *
     * <p>It uses the real ShardConsumer and its real states (PROCESSING, then SHUTTING_DOWN, then
     * SHUTDOWN_COMPLETE), with {@link #recordingShutdownTaskFactory} configured so the first task succeeds.
     */
    @Test(timeout = 10000L)
    public void testLeaseLostDuringShardEndShutdown_doesNotStartSecondShutdownTask() throws Exception {
        final CountDownLatch shutdownTaskStarted = new CountDownLatch(1);
        final CountDownLatch releaseShutdownTask = new CountDownLatch(1);
        // The reason each shutdown task is created with, in order. A thread-safe (concurrent) ArrayList, because the
        // consumer's executor thread adds to it when it creates a task, and this test thread reads it.
        final List<ShutdownReason> shutdownTaskReasons = new CopyOnWriteArrayList<>();
        final ConsumerTaskFactory taskFactory =
                recordingShutdownTaskFactory(shutdownTaskReasons, shutdownTaskStarted, releaseShutdownTask, false);
        final ShardConsumer consumer = createShardConsumerAtShardEnd(taskFactory);

        // Step 1: the consumer moves to SHUTTING_DOWN, and creates the SHARD_END shutdown task (recording SHARD_END)
        // and runs it on its executor thread.
        final CompletableFuture<Boolean> firstTaskStep = consumer.shutdownComplete();

        // Wait until the first shutdown task is running. The task itself signals this: recordingShutdownTaskFactory
        // makes it count down shutdownTaskStarted as soon as it starts, then wait on releaseShutdownTask. So this
        // await() returns almost immediately, and the task stays blocked until this thread counts down
        // releaseShutdownTask below. Whatever this thread does in between happens while the first task is running.
        shutdownTaskStarted.await();
        // The lease is lost mid-task (for example, the leader evicts it and this worker's renewal fails), so on the
        // Scheduler's next pass the shard isn't assigned and cleanupShardConsumers calls ShardConsumer.leaseLost().
        // That only raises the consumer's shutdown reason; it returns false because the first shutdown task is
        // still running, so the consumer isn't shut down.
        assertFalse("shutdown still in progress", consumer.leaseLost());
        assertEquals(ShutdownReason.LEASE_LOST, consumer.shutdownReason());
        releaseShutdownTask.countDown(); // let the first shutdown task finish

        // The first task returns, but step 1 doesn't apply its outcome, so the consumer isn't shut down yet.
        assertFalse(firstTaskStep.get());
        // Step 2: applies the success. In SHUTTING_DOWN, that moves the consumer to SHUTDOWN_COMPLETE (and so does the
        // raised LEASE_LOST reason), so it's shut down and no further task is created.
        assertTrue(consumer.shutdownComplete().get());

        assertEquals(Collections.singletonList(ShutdownReason.SHARD_END), shutdownTaskReasons);
        assertEquals(ShutdownReason.LEASE_LOST, consumer.shutdownReason());
    }

    /**
     * Same as above, but the first SHARD_END shutdown task fails, for example because the record processor's
     * shardEnded() threw. In the SHUTTING_DOWN state, a failed task doesn't move the consumer anywhere: it stays in
     * SHUTTING_DOWN and creates a new (retry) shutdown task. The retry copies the consumer's current shutdown reason
     * when it's created, and the reason was raised to LEASE_LOST while the first task ran, so the retry is a
     * LEASE_LOST shutdown task, not a SHARD_END one.
     *
     * <p>The ShardConsumer only sees that the task failed, not where it failed: whether shardEnded() threw before or
     * after its SHARD_END checkpoint makes no difference here, and the retry is LEASE_LOST either way. That
     * difference only matters inside the retry's ShutdownTask. This is why ShutdownTask checks for an
     * already-completed SHARD_END checkpoint before it looks at the shutdown reason: if shardEnded() had already
     * checkpointed, this LEASE_LOST retry must not call the record processor's leaseLost() (see
     * ShutdownTaskTest#testLeaseLostRetryAfterShardEndedThrowsAndLeaseEvicted_noFurtherApplicationCalls); if it
     * hadn't, leaseLost() is correct (see
     * ShutdownTaskTest#testLeaseLostAfterShardEndedThrewBeforeCheckpoint_leaseLostRetry_callsLeaseLost).
     *
     * <p>When this happens in practice: the lease is lost while the first task runs, the task fails, and the
     * Scheduler calls ShardConsumer.leaseLost() before the retry is created. The task can fail in two ways:
     * <ul>
     *   <li>The lease was lost before the SHARD_END checkpoint (expiry or a rebalance; not the leader, which only
     *       acts on leases at SHARD_END), so the checkpoint failed. The LEASE_LOST retry then calls the record
     *       processor's leaseLost(): a real lease loss (row 6 of the table above).</li>
     *   <li>The checkpoint succeeded, the lease was lost (for example, the leader evicted it), and shardEnded()'s
     *       later work threw. The LEASE_LOST retry then makes no further application calls (row 4).</li>
     * </ul>
     *
     * <p>Compared with the other tests in this section: like the next test, the first task fails, but here the
     * lease is lost while it runs, so the retry is LEASE_LOST instead of SHARD_END.
     */
    @Test(timeout = 10000L)
    public void testFailedShardEndShutdown_schedulerSawLeaseLossBeforeRetry_retryIsLeaseLostShutdownTask()
            throws Exception {
        final CountDownLatch shutdownTaskStarted = new CountDownLatch(1);
        final CountDownLatch releaseShutdownTask = new CountDownLatch(1);
        // The reason each shutdown task is created with, in order. A thread-safe (concurrent) ArrayList, because the
        // consumer's executor thread adds to it when it creates a task, and this test thread reads it.
        final List<ShutdownReason> shutdownTaskReasons = new CopyOnWriteArrayList<>();
        final ConsumerTaskFactory taskFactory =
                recordingShutdownTaskFactory(shutdownTaskReasons, shutdownTaskStarted, releaseShutdownTask, true);
        final ShardConsumer consumer = createShardConsumerAtShardEnd(taskFactory);

        // Step 1: the consumer moves to SHUTTING_DOWN, and creates the SHARD_END shutdown task (recording SHARD_END)
        // and runs it on its executor thread. This one is configured to fail when it's released.
        final CompletableFuture<Boolean> firstTaskStep = consumer.shutdownComplete();

        // Wait until the first shutdown task is running (it counts down shutdownTaskStarted as soon as it starts,
        // then waits on releaseShutdownTask; see the test above).
        shutdownTaskStarted.await();
        // The lease is lost mid-task (for example, the leader evicts it and this worker's renewal fails), so on the
        // Scheduler's next pass the shard isn't assigned and cleanupShardConsumers calls ShardConsumer.leaseLost().
        // That only raises the consumer's shutdown reason; it returns false because the first shutdown task is
        // still running, so the consumer isn't shut down.
        assertFalse("shutdown still in progress", consumer.leaseLost());
        assertEquals(ShutdownReason.LEASE_LOST, consumer.shutdownReason());
        releaseShutdownTask.countDown(); // let the first shutdown task finish, with a failure

        // The first task returns its failure, but step 1 doesn't apply it, so the consumer isn't shut down yet.
        assertFalse(firstTaskStep.get());
        // Step 2: applies the failure, so the consumer stays in SHUTTING_DOWN, and creates the retry (recording
        // LEASE_LOST, since the Scheduler has already seen the lease loss), which succeeds right away.
        assertFalse(consumer.shutdownComplete().get());
        // Step 3: applies the retry's success and moves the consumer to SHUTDOWN_COMPLETE.
        assertTrue(consumer.shutdownComplete().get());

        // The first task was created with SHARD_END, before the reason was raised; the retry with LEASE_LOST.
        assertEquals(Arrays.asList(ShutdownReason.SHARD_END, ShutdownReason.LEASE_LOST), shutdownTaskReasons);
    }

    /**
     * The counterpart of the test above: the first SHARD_END shutdown task fails, but the Scheduler doesn't call
     * ShardConsumer.leaseLost() while it runs, so nothing raises the consumer's shutdown reason and the retry is
     * created with SHARD_END. That's how retries worked before leader-side SHARD_END cleanup was introduced; this
     * change doesn't affect it. This test only checks the retry's reason (its tasks are mocks); what the retry does
     * is covered in ShutdownTaskTest.
     *
     * <p>The Scheduler hasn't called leaseLost() in two situations, and both get this same SHARD_END retry:
     * <ul>
     *   <li>The lease is still held (rows 1 and 2 of the table above). The retry runs ShutdownTask's SHARD_END path,
     *       unchanged by this change: it reads the lease from the lease table, and if it's already at SHARD_END, it
     *       skips shardEnded() and queues the lease for cleanup; otherwise it calls shardEnded() again.</li>
     *   <li>The lease was lost, but the Scheduler hasn't noticed yet: this worker's renewer has dropped it, and the
     *       Scheduler pass that would call leaseLost() hasn't run when the retry is created. The retry finds no held
     *       lease, and what it does depends on whether shardEnded() had checkpointed:
     *       <ul>
     *         <li>It had (row 3): the check this change added at the top of ShutdownTask.call() applies, and the
     *             retry finishes without calling the record processor.</li>
     *         <li>It hadn't (row 5): a real lease loss. The SHARD_END path treats the lease as not owned and calls
     *             the record processor's leaseLost(). (Row 6 is the same case with a LEASE_LOST retry, when the
     *             Scheduler noticed first; see the test above.)</li>
     *       </ul></li>
     * </ul>
     *
     * <p>Why both situations get the same retry: the ShardConsumer and the ShutdownTask learn about a lost lease in
     * different ways, and use it for different things.
     * <ul>
     *   <li>The ShardConsumer only decides whether to create a retry (from the failed task's result) and which
     *       reason to give it (its own shutdownReason field). It never looks at which leases this worker holds. It
     *       only learns about a lost lease when the Scheduler calls ShardConsumer.leaseLost(), which raises that
     *       reason. Until then, the consumer is in the same state whether or not the renewer has dropped the lease,
     *       so it creates the same SHARD_END retry.</li>
     *   <li>The ShutdownTask doesn't rely on the consumer for this. At the start of call(), for every reason, it
     *       reads getCurrentlyHeldLease() to see whether this worker still holds the lease. That's how a SHARD_END
     *       retry finds out the lease was lost. A LEASE_LOST retry reads it too, because the check this change added
     *       runs before the task looks at its reason, and it needs two facts the reason doesn't carry: the lease isn't
     *       held (from this read) and this processor already checkpointed SHARD_END (from its checkpointer).</li>
     * </ul>
     *
     * <p>Compared with the other tests in this section: like the test above, the first task fails, but here the
     * reason isn't raised, so the retry is SHARD_END. The two tests take the same three steps; the only difference
     * is whether leaseLost() is called while the first task runs.
     */
    @Test(timeout = 10000L)
    public void testFailedShardEndShutdown_schedulerHasNotSeenLeaseLossBeforeRetry_retryIsShardEndShutdownTask()
            throws Exception {
        final CountDownLatch shutdownTaskStarted = new CountDownLatch(1);
        final CountDownLatch releaseShutdownTask = new CountDownLatch(1);
        // The reason each shutdown task is created with, in order. A thread-safe (concurrent) ArrayList, because the
        // consumer's executor thread adds to it when it creates a task, and this test thread reads it.
        final List<ShutdownReason> shutdownTaskReasons = new CopyOnWriteArrayList<>();
        final ConsumerTaskFactory taskFactory =
                recordingShutdownTaskFactory(shutdownTaskReasons, shutdownTaskStarted, releaseShutdownTask, true);
        final ShardConsumer consumer = createShardConsumerAtShardEnd(taskFactory);

        // Step 1: the consumer moves to SHUTTING_DOWN, and creates the SHARD_END shutdown task (recording SHARD_END)
        // and runs it on its executor thread. This one is configured to fail when it's released.
        final CompletableFuture<Boolean> firstTaskStep = consumer.shutdownComplete();

        // Wait until the first shutdown task is running, then let it finish, with a failure. Nothing calls
        // leaseLost() in between (the lease is still held, or the Scheduler hasn't seen it lost before the retry is
        // created), so nothing raises the shutdown reason.
        shutdownTaskStarted.await();
        releaseShutdownTask.countDown();

        // The first task returns its failure, but step 1 doesn't apply it, so the consumer isn't shut down yet.
        assertFalse(firstTaskStep.get());
        // Step 2: applies the failure, so the consumer stays in SHUTTING_DOWN, and creates the retry (recording
        // SHARD_END, since the reason was never raised), which succeeds right away.
        assertFalse(consumer.shutdownComplete().get());
        // Step 3: applies the retry's success and moves the consumer to SHUTDOWN_COMPLETE.
        assertTrue(consumer.shutdownComplete().get());

        assertEquals(Arrays.asList(ShutdownReason.SHARD_END, ShutdownReason.SHARD_END), shutdownTaskReasons);
        assertEquals(ShutdownReason.SHARD_END, consumer.shutdownReason());
    }

    /**
     * A ShardConsumer in the real PROCESSING state that has reached the end of its shard, as it is after its last
     * ProcessTask returns END_OF_SHARD.
     */
    private ShardConsumer createShardConsumerAtShardEnd(final ConsumerTaskFactory taskFactory) {
        final ShardConsumer consumer = new ShardConsumer(
                recordsPublisher,
                executorService,
                shardInfo,
                logWarningForTaskAfterMillis,
                shardConsumerArgument,
                ShardConsumerState.PROCESSING.consumerState(),
                1,
                taskExecutionListener,
                0,
                taskFactory);
        consumer.markForShutdown(ShutdownReason.SHARD_END);
        return consumer;
    }

    /**
     * A mocked task factory for driving a real ShardConsumer through a shutdown. The consumer asks its factory for
     * each shutdown task, including each retry, and runs the task on its executor thread. This factory ignores the
     * {@code argument} and {@code input} parameters and records {@code consumer.shutdownReason()}, the reason the
     * real factory would create the task with.
     *
     * <p>The first shutdown task counts down {@code started} and then waits for {@code release}, so the test controls
     * when it starts and finishes, and can do anything before, during, or after it (for example, call
     * {@link ShardConsumer#leaseLost()} while it runs, as the Scheduler does after the lease is lost). It then
     * succeeds, or fails if {@code firstTaskFails}, which makes the consumer retry. Retries succeed immediately, so
     * the consumer finishes shutting down.
     */
    private ConsumerTaskFactory recordingShutdownTaskFactory(
            final List<ShutdownReason> shutdownTaskReasons,
            final CountDownLatch started,
            final CountDownLatch release,
            final boolean firstTaskFails) {
        final ConsumerTaskFactory taskFactory = mock(ConsumerTaskFactory.class);
        when(taskFactory.createShutdownTask(any(), any(), any())).thenAnswer(invocation -> {
            final boolean first = shutdownTaskReasons.isEmpty();
            shutdownTaskReasons.add(((ShardConsumer) invocation.getArguments()[1]).shutdownReason());
            final ConsumerTask task = mock(ConsumerTask.class);
            when(task.taskType()).thenReturn(TaskType.SHUTDOWN);
            when(task.call()).thenAnswer(call -> {
                if (!first) {
                    return new TaskResult(null);
                }
                started.countDown();
                release.await();
                return new TaskResult(firstTaskFails ? new RuntimeException("shardEnded threw") : null);
            });
            return task;
        });
        return taskFactory;
    }

    // =====================================================================================================
    // End of: SHARD_END shutdown racing with a lease loss
    // =====================================================================================================

    private void mockSuccessfulShutdown(CyclicBarrier taskCallBarrier) {
        mockSuccessfulShutdown(taskCallBarrier, null);
    }

    private void mockSuccessfulShutdown(CyclicBarrier taskArriveBarrier, CyclicBarrier taskDepartBarrier) {
        when(shutdownState.createTask(eq(shardConsumerArgument), any(), any(), any()))
                .thenReturn(shutdownTask);
        when(shutdownState.taskType()).thenReturn(TaskType.SHUTDOWN);
        when(shutdownTask.call()).thenAnswer(i -> {
            awaitBarrier(taskArriveBarrier);
            awaitBarrier(taskDepartBarrier);
            return new TaskResult(null);
        });
        when(shutdownState.shutdownTransition(any())).thenReturn(shutdownCompleteState);
        when(shutdownState.state()).thenReturn(ConsumerStates.ShardConsumerState.SHUTTING_DOWN);

        when(shutdownCompleteState.isTerminal()).thenReturn(true);
    }

    private void mockSuccessfulProcessing(CyclicBarrier taskCallBarrier) {
        mockSuccessfulProcessing(taskCallBarrier, null);
    }

    private void mockSuccessfulProcessing(CyclicBarrier taskCallBarrier, CyclicBarrier taskInterlockBarrier) {
        when(processingState.createTask(eq(shardConsumerArgument), any(), any(), any()))
                .thenReturn(processingTask);
        when(processingState.taskType()).thenReturn(TaskType.PROCESS);
        when(processingTask.taskType()).thenReturn(TaskType.PROCESS);
        when(processingTask.call()).thenAnswer(i -> {
            awaitBarrier(taskCallBarrier);
            awaitBarrier(taskInterlockBarrier);
            return processingTaskResult;
        });
        when(processingTaskResult.getException()).thenReturn(null);
        when(processingState.successTransition()).thenReturn(processingState);
        when(processingState.shutdownTransition(any())).thenReturn(shutdownState);
        when(processingState.state()).thenReturn(ConsumerStates.ShardConsumerState.PROCESSING);
    }

    private void mockSuccessfulInitializeWithFailureTransition() {
        mockSuccessfulInitialize(null, null);
        when(initialState.failureTransition()).thenReturn(initialState);
    }

    private void mockSuccessfulInitialize(CyclicBarrier taskCallBarrier) {
        mockSuccessfulInitialize(taskCallBarrier, null);
    }

    private void mockSuccessfulInitialize(CyclicBarrier taskCallBarrier, CyclicBarrier taskInterlockBarrier) {
        when(initialState.createTask(eq(shardConsumerArgument), any(), any(), any()))
                .thenReturn(initializeTask);
        when(initialState.taskType()).thenReturn(TaskType.INITIALIZE);
        when(initializeTask.taskType()).thenReturn(TaskType.INITIALIZE);
        when(initializeTask.call()).thenAnswer(i -> {
            awaitBarrier(taskCallBarrier);
            awaitBarrier(taskInterlockBarrier);
            return initializeTaskResult;
        });
        when(initializeTaskResult.getException()).thenReturn(null);
        when(initialState.successTransition()).thenReturn(processingState);
        when(initialState.state()).thenReturn(ConsumerStates.ShardConsumerState.INITIALIZING);
    }

    private void mockSuccessfulUnblockOnParentsWithFailureTransition() {
        mockSuccessfulUnblockOnParents();
        when(blockedOnParentsState.failureTransition()).thenReturn(blockedOnParentsState);
    }

    private void mockSuccessfulUnblockOnParents() {
        when(blockedOnParentsState.createTask(eq(shardConsumerArgument), any(), any(), any()))
                .thenReturn(blockedOnParentsTask);
        when(blockedOnParentsState.taskType()).thenReturn(TaskType.BLOCK_ON_PARENT_SHARDS);
        when(blockedOnParentsTask.call()).thenAnswer(i -> blockOnParentsTaskResult);
        when(blockOnParentsTaskResult.getException()).thenReturn(null);
        when(blockedOnParentsState.successTransition()).thenReturn(initialState);
        when(blockedOnParentsState.state()).thenReturn(ShardConsumerState.WAITING_ON_PARENT_SHARDS);
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

    private Object directlyExecuteRunnable(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        Runnable runnable = (Runnable) args[0];
        runnable.run();
        return null;
    }

    private ShardConsumer createShardConsumer(final RecordsPublisher publisher) {
        return createShardConsumer(publisher, executorService, initialState);
    }

    private ShardConsumer createShardConsumer(final ExecutorService executorService, final ConsumerState state) {
        return createShardConsumer(recordsPublisher, executorService, state);
    }

    private ShardConsumer createShardConsumer(
            final RecordsPublisher publisher, final ExecutorService executorService, final ConsumerState state) {
        return new ShardConsumer(
                publisher,
                executorService,
                shardInfo,
                logWarningForTaskAfterMillis,
                shardConsumerArgument,
                state,
                1,
                taskExecutionListener,
                0,
                new KinesisConsumerTaskFactory());
    }
}
