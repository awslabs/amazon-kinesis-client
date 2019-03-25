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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;

import lombok.Getter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Unit tests of {@link ShardConsumer}.
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class ShardConsumerTest {

    @Rule
    public TestName testName = new TestName();

    private CyclicBarrierShardConsumerContext testContext;

    public static Logger getLog() {
        return log;
    }

    @Before
    public void before() {
        this.testContext = new CyclicBarrierShardConsumerContext(testName);
    }

    @After
    public void after() {
        List<Runnable> remainder = testContext.executorService().shutdownNow();
        assertThat(remainder.isEmpty(), equalTo(true));
    }

    @Test
    public void simpleTest() throws Exception {
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        testContext.mockSuccessfulInitialize(null);

        testContext.mockSuccessfulProcessing(taskCallBarrier);

        testContext.mockSuccessfulShutdown(null);

        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(testContext.processRecordsInput());
        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), testContext.logWarningForTaskAfterMillis(),
                testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),0);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        consumer.leaseLost();
        boolean shutdownComplete = consumer.shutdownComplete().get();
        while (!shutdownComplete) {
            shutdownComplete = consumer.shutdownComplete().get();
        }

        verify(cache.subscription, times(3)).request(anyLong());
        verify(cache.subscription).cancel();
        verify(testContext.processingState(), times(2)).createTask(eq(testContext.shardConsumerArgument()), eq(consumer), any());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(2)).beforeTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownTaskInput());

        testContext.initialTaskInput(testContext.initialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.processTaskInput(testContext.processTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.shutdownTaskInput(testContext.shutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(2)).afterTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

    @Test(timeout = 1000L)
    public void testLeaseLossIsNonBlocking() throws Exception {
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);
        CyclicBarrier processingTaskInterlock = new CyclicBarrier(2);

        testContext.mockSuccessfulInitialize(null);
        testContext.mockSuccessfulProcessing(taskCallBarrier, processingTaskInterlock);
        testContext.mockSuccessfulShutdown(null);

        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(testContext.processRecordsInput());
        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), testContext.logWarningForTaskAfterMillis(),
                testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),0);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        getLog().debug("Setup complete publishing entry");
        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);
        consumer.leaseLost();

        //
        // This will block if a lock is held on ShardConsumer#this
        //
        consumer.executeLifecycle();
        assertThat(consumer.isShutdown(), equalTo(false));

        getLog().debug("Release processing task interlock");
        CyclicBarrierTestPublisher.awaitAndResetBarrier(processingTaskInterlock);

        while(!consumer.isShutdown()) {
            consumer.executeLifecycle();
            Thread.yield();
        }

        verify(cache.subscription, times(1)).request(anyLong());
        verify(cache.subscription).cancel();
        verify(testContext.processingState(), times(1)).createTask(eq(testContext.shardConsumerArgument()), eq(consumer), any());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownTaskInput());

        testContext.initialTaskInput(testContext.initialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.processTaskInput(testContext.processTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.shutdownTaskInput(testContext.shutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

    @Test
    public void testDataArrivesAfterProcessing2() throws Exception {

        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        testContext.mockSuccessfulInitialize(null);
        testContext.mockSuccessfulProcessing(taskCallBarrier);
        testContext.mockSuccessfulShutdown(null);

        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(testContext.processRecordsInput());
        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), testContext.logWarningForTaskAfterMillis(),
                testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),0);

        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);

        verify(testContext.processingState()).createTask(any(), any(), any());
        verify(testContext.processingTask()).call();

        cache.awaitRequest();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);
        verify(testContext.processingState(), times(2)).createTask(any(), any(), any());
        verify(testContext.processingTask(), times(2)).call();

        cache.awaitRequest();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        getLog().info("Starting shutdown");
        consumer.leaseLost();
        boolean shutdownComplete;
        do {
            shutdownComplete = consumer.shutdownComplete().get();
        } while (!shutdownComplete);

        verify(testContext.processingState(), times(3)).createTask(any(), any(), any());
        verify(testContext.processingTask(), times(3)).call();
        verify(testContext.processingState()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(testContext.shutdownState()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(3)).beforeTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownTaskInput());

        testContext.initialTaskInput(testContext.initialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.processTaskInput(testContext.processTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.shutdownTaskInput(testContext.shutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(3)).afterTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

    @SuppressWarnings("unchecked")
    @Test
    @Ignore
    public final void testInitializationStateUponFailure() throws Exception {
        ShardConsumer consumer = new ShardConsumer(testContext.recordsPublisher(), testContext.executorService(), testContext.shardInfo(),
                testContext.logWarningForTaskAfterMillis(), testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),0);

        when(testContext.initialState().createTask(eq(testContext.shardConsumerArgument()), eq(consumer), any())).thenReturn(testContext.initializeTask());
        when(testContext.initializeTask().call()).thenReturn(new TaskResult(new Exception("Bad")));
        when(testContext.initializeTask().taskType()).thenReturn(TaskType.INITIALIZE);
        when(testContext.initialState().failureTransition()).thenReturn(testContext.initialState());

        CyclicBarrier taskBarrier = new CyclicBarrier(2);

        when(testContext.initialState().requiresDataAvailability()).thenAnswer(i -> {
            taskBarrier.await();
            return false;
        });

        consumer.executeLifecycle();
        for (int i = 0; i < 4; ++i) {
            CyclicBarrierTestPublisher.awaitAndResetBarrier(taskBarrier);
        }

        verify(testContext.initialState(), times(5)).createTask(eq(testContext.shardConsumerArgument()), eq(consumer), any());
        verify(testContext.initialState(), never()).successTransition();
        verify(testContext.initialState(), never()).shutdownTransition(any());
    }

    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test(expected = RejectedExecutionException.class)
    public final void testInitializationStateUponSubmissionFailure() throws Exception {

        ExecutorService failingService = mock(ExecutorService.class);
        ShardConsumer consumer = new ShardConsumer(testContext.recordsPublisher(), failingService, testContext.shardInfo(),
                testContext.logWarningForTaskAfterMillis(), testContext.shardConsumerArgument(), testContext.initialState(), t -> t, 1, testContext.taskExecutionListener(),0);

        doThrow(new RejectedExecutionException()).when(failingService).execute(any());

        boolean initComplete;
        do {
            initComplete = consumer.initializeComplete().get();
        } while (!initComplete);
        verifyZeroInteractions(testContext.taskExecutionListener());
    }

    @Test
    public void testErrorThrowableInInitialization() throws Exception {
        ShardConsumer consumer = new ShardConsumer(testContext.recordsPublisher(), testContext.executorService(), testContext.shardInfo(),
                testContext.logWarningForTaskAfterMillis(), testContext.shardConsumerArgument(), testContext.initialState(), t -> t, 1, testContext.taskExecutionListener(),0);

        when(testContext.initialState().createTask(any(), any(), any())).thenReturn(testContext.initializeTask());
        when(testContext.initialState().taskType()).thenReturn(TaskType.INITIALIZE);
        when(testContext.initializeTask().call()).thenAnswer(i -> {
            throw new Error("Error");
        });

        try {
            consumer.initializeComplete().get();
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(Error.class));
        }
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

    @Test
    public void testRequestedShutdownWhileQuiet() throws Exception {

        CyclicBarrier taskBarrier = new CyclicBarrier(2);

        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(testContext.processRecordsInput());
        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), testContext.logWarningForTaskAfterMillis(),
                testContext.shardConsumerArgument(), testContext.initialState(), t -> t, 1, testContext.taskExecutionListener(),0);

        testContext.mockSuccessfulInitialize(null);

        testContext.mockSuccessfulProcessing(taskBarrier);

        when(testContext.processingState().shutdownTransition(eq(ShutdownReason.REQUESTED))).thenReturn(testContext.shutdownRequestedState());
        when(testContext.shutdownRequestedState().requiresDataAvailability()).thenReturn(false);
        when(testContext.shutdownRequestedState().createTask(any(), any(), any())).thenReturn(testContext.shutdownRequestedTask());
        when(testContext.shutdownRequestedState().taskType()).thenReturn(TaskType.SHUTDOWN_NOTIFICATION);
        when(testContext.shutdownRequestedTask().call()).thenReturn(new TaskResult(null));

        when(testContext.shutdownRequestedState().shutdownTransition(eq(ShutdownReason.REQUESTED)))
                .thenReturn(testContext.shutdownRequestedAwaitState());
        when(testContext.shutdownRequestedState().shutdownTransition(eq(ShutdownReason.LEASE_LOST))).thenReturn(testContext.shutdownState());
        when(testContext.shutdownRequestedAwaitState().requiresDataAvailability()).thenReturn(false);
        when(testContext.shutdownRequestedAwaitState().createTask(any(), any(), any())).thenReturn(null);
        when(testContext.shutdownRequestedAwaitState().shutdownTransition(eq(ShutdownReason.REQUESTED)))
                .thenReturn(testContext.shutdownRequestedState());
        when(testContext.shutdownRequestedAwaitState().shutdownTransition(eq(ShutdownReason.LEASE_LOST))).thenReturn(testContext.shutdownState());
        when(testContext.shutdownRequestedAwaitState().taskType()).thenReturn(TaskType.SHUTDOWN_COMPLETE);

        testContext.mockSuccessfulShutdown(null);

        boolean init = consumer.initializeComplete().get();
        while (!init) {
            init = consumer.initializeComplete().get();
        }

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskBarrier);
        cache.awaitRequest();

        cache.publish();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskBarrier);
        cache.awaitRequest();

        consumer.gracefulShutdown(testContext.shutdownNotification());
        boolean shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(false));
        shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(false));

        consumer.leaseLost();
        shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(false));
        shutdownComplete = consumer.shutdownComplete().get();
        assertThat(shutdownComplete, equalTo(true));

        verify(testContext.processingState(), times(2)).createTask(any(), any(), any());
        verify(testContext.shutdownRequestedState(), never()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(testContext.shutdownRequestedState()).createTask(any(), any(), any());
        verify(testContext.shutdownRequestedState()).shutdownTransition(eq(ShutdownReason.REQUESTED));
        verify(testContext.shutdownRequestedAwaitState()).createTask(any(), any(), any());
        verify(testContext.shutdownRequestedAwaitState()).shutdownTransition(eq(ShutdownReason.LEASE_LOST));
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(2)).beforeTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownRequestedTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownRequestedAwaitTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownTaskInput());

        testContext.initialTaskInput(testContext.initialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.processTaskInput(testContext.processTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.shutdownRequestedTaskInput(testContext.shutdownRequestedTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.shutdownTaskInput(testContext.shutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        // No task is created/run for this shutdownRequestedAwaitState, so there's no task outcome.

        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(2)).afterTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownRequestedTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownRequestedAwaitTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

    @Test
    public void testExceptionInProcessingStopsRequests() throws Exception {
        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(testContext.processRecordsInput());

        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), Optional.of(1L),
                testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),0);

        testContext.mockSuccessfulInitialize(null);
        testContext.mockSuccessfulProcessing(null);

        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);
        final RuntimeException expectedException = new RuntimeException("Whee");
        when(testContext.processingTask().call()).thenAnswer(a -> {
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
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskCallBarrier);
        cache.awaitRequest();

        Throwable healthCheckOutcome = consumer.healthCheck();

        assertThat(healthCheckOutcome, equalTo(expectedException));

        verify(cache.subscription, times(2)).request(anyLong());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.processTaskInput());

        testContext.initialTaskInput(testContext.initialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.initialTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

    @Test
    public void testLongRunningTasks() throws Exception {

        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(testContext.processRecordsInput());

        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), Optional.of(1L),
                testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),0);

        CyclicBarrier taskArriveBarrier = new CyclicBarrier(2);
        CyclicBarrier taskDepartBarrier = new CyclicBarrier(2);

        testContext.mockSuccessfulInitialize(taskArriveBarrier, taskDepartBarrier);
        testContext.mockSuccessfulProcessing(taskArriveBarrier, taskDepartBarrier);
        testContext.mockSuccessfulShutdown(taskArriveBarrier, taskDepartBarrier);

        CompletableFuture<Boolean> initSuccess = consumer.initializeComplete();

        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskArriveBarrier);
        assertThat(consumer.taskRunningTime(), notNullValue());
        consumer.healthCheck();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskDepartBarrier);

        assertThat(initSuccess.get(), equalTo(false));
        verify(testContext.initializeTask()).call();

        initSuccess = consumer.initializeComplete();
        verify(testContext.initializeTask()).call();
        assertThat(initSuccess.get(), equalTo(true));
        consumer.healthCheck();

        assertThat(consumer.taskRunningTime(), nullValue());

        consumer.subscribe();
        cache.awaitInitialSetup();

        cache.publish();

        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskArriveBarrier);
        Instant previousTaskStartTime = consumer.taskDispatchedAt();
        assertThat(consumer.taskRunningTime(), notNullValue());
        consumer.healthCheck();
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskDepartBarrier);

        consumer.healthCheck();

        cache.requestBarrier.await();
        assertThat(consumer.taskRunningTime(), nullValue());
        cache.requestBarrier.reset();

        cache.publish();

        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskArriveBarrier);
        Instant currentTaskStartTime = consumer.taskDispatchedAt();
        assertThat(currentTaskStartTime, not(equalTo(previousTaskStartTime)));
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskDepartBarrier);

        cache.requestBarrier.await();
        assertThat(consumer.taskRunningTime(), nullValue());
        cache.requestBarrier.reset();

        consumer.leaseLost();

        assertThat(consumer.isShutdownRequested(), equalTo(true));
        CompletableFuture<Boolean> shutdownComplete = consumer.shutdownComplete();

        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskArriveBarrier);
        assertThat(consumer.taskRunningTime(), notNullValue());
        CyclicBarrierTestPublisher.awaitAndResetBarrier(taskDepartBarrier);

        assertThat(shutdownComplete.get(), equalTo(false));

        shutdownComplete = consumer.shutdownComplete();
        assertThat(shutdownComplete.get(), equalTo(true));

        assertThat(consumer.taskRunningTime(), nullValue());
        consumer.healthCheck();

        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(2)).beforeTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).beforeTaskExecution(testContext.shutdownTaskInput());

        testContext.initialTaskInput(testContext.initialTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.processTaskInput(testContext.processTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());
        testContext.shutdownTaskInput(testContext.shutdownTaskInput().toBuilder().taskOutcome(TaskOutcome.SUCCESSFUL).build());

        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.initialTaskInput());
        verify(testContext.taskExecutionListener(), times(2)).afterTaskExecution(testContext.processTaskInput());
        verify(testContext.taskExecutionListener(), times(1)).afterTaskExecution(testContext.shutdownTaskInput());
        verifyNoMoreInteractions(testContext.taskExecutionListener());
    }

}
