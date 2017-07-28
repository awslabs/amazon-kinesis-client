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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.verification.VerificationMode;

@RunWith(MockitoJUnitRunner.class)
public class GracefulShutdownCoordinatorTest {

    @Mock
    private CountDownLatch shutdownCompleteLatch;
    @Mock
    private CountDownLatch notificationCompleteLatch;
    @Mock
    private Worker worker;
    @Mock
    private Callable<GracefulShutdownContext> contextCallable;
    @Mock
    private ConcurrentMap<ShardInfo, ShardConsumer> shardInfoConsumerMap;

    @Test
    public void testAllShutdownCompletedAlready() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(shutdownCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

        assertThat(requestedShutdownCallable.call(), equalTo(true));
        verify(shutdownCompleteLatch).await(anyLong(), any(TimeUnit.class));
        verify(notificationCompleteLatch).await(anyLong(), any(TimeUnit.class));
        verify(worker).shutdown();
    }

    @Test
    public void testNotificationNotCompletedYet() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        mockLatchAwait(notificationCompleteLatch, false, true);
        when(notificationCompleteLatch.getCount()).thenReturn(1L, 0L);
        mockLatchAwait(shutdownCompleteLatch, true);
        when(shutdownCompleteLatch.getCount()).thenReturn(1L, 1L, 0L);

        when(worker.isShutdownComplete()).thenReturn(false, true);
        mockShardInfoConsumerMap(1, 0);

        assertThat(requestedShutdownCallable.call(), equalTo(true));
        verify(notificationCompleteLatch, times(2)).await(anyLong(), any(TimeUnit.class));
        verify(notificationCompleteLatch).getCount();

        verify(shutdownCompleteLatch).await(anyLong(), any(TimeUnit.class));
        verify(shutdownCompleteLatch, times(2)).getCount();

        verify(worker).shutdown();
    }

    @Test
    public void testShutdownNotCompletedYet() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        mockLatchAwait(notificationCompleteLatch, true);
        mockLatchAwait(shutdownCompleteLatch, false, true);
        when(shutdownCompleteLatch.getCount()).thenReturn(1L, 0L);

        when(worker.isShutdownComplete()).thenReturn(false, true);
        mockShardInfoConsumerMap(1, 0);

        assertThat(requestedShutdownCallable.call(), equalTo(true));
        verify(notificationCompleteLatch).await(anyLong(), any(TimeUnit.class));
        verify(notificationCompleteLatch, never()).getCount();

        verify(shutdownCompleteLatch, times(2)).await(anyLong(), any(TimeUnit.class));
        verify(shutdownCompleteLatch, times(2)).getCount();

        verify(worker).shutdown();
    }

    @Test
    public void testMultipleAttemptsForNotification() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        mockLatchAwait(notificationCompleteLatch, false, false, true);
        when(notificationCompleteLatch.getCount()).thenReturn(2L, 1L, 0L);

        mockLatchAwait(shutdownCompleteLatch, true);
        when(shutdownCompleteLatch.getCount()).thenReturn(2L, 2L, 1L, 1L, 0L);

        when(worker.isShutdownComplete()).thenReturn(false, false, false, true);
        mockShardInfoConsumerMap(2, 1, 0);

        assertThat(requestedShutdownCallable.call(), equalTo(true));

        verifyLatchAwait(notificationCompleteLatch, 3);
        verify(notificationCompleteLatch, times(2)).getCount();

        verifyLatchAwait(shutdownCompleteLatch, 1);
        verify(shutdownCompleteLatch, times(4)).getCount();
    }

    @Test
    public void testWorkerAlreadyShutdownAtNotification() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        mockLatchAwait(notificationCompleteLatch, false, true);
        when(notificationCompleteLatch.getCount()).thenReturn(1L, 0L);

        mockLatchAwait(shutdownCompleteLatch, true);
        when(shutdownCompleteLatch.getCount()).thenReturn(1L, 1L, 0L);

        when(worker.isShutdownComplete()).thenReturn(true);
        mockShardInfoConsumerMap(0);

        assertThat(requestedShutdownCallable.call(), equalTo(false));

        verifyLatchAwait(notificationCompleteLatch);
        verify(notificationCompleteLatch).getCount();

        verifyLatchAwait(shutdownCompleteLatch, never());
        verify(shutdownCompleteLatch, times(3)).getCount();
    }

    @Test
    public void testWorkerAlreadyShutdownAtComplete() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        mockLatchAwait(notificationCompleteLatch, true);

        mockLatchAwait(shutdownCompleteLatch, false, true);
        when(shutdownCompleteLatch.getCount()).thenReturn(1L, 1L, 1L);

        when(worker.isShutdownComplete()).thenReturn(true);
        mockShardInfoConsumerMap(0);

        assertThat(requestedShutdownCallable.call(), equalTo(false));

        verifyLatchAwait(notificationCompleteLatch);
        verify(notificationCompleteLatch, never()).getCount();

        verifyLatchAwait(shutdownCompleteLatch);
        verify(shutdownCompleteLatch, times(3)).getCount();
    }

    @Test
    public void testNotificationInterrupted() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
        when(notificationCompleteLatch.getCount()).thenReturn(1L);

        when(shutdownCompleteLatch.getCount()).thenReturn(1L);

        assertThat(requestedShutdownCallable.call(), equalTo(false));
        verifyLatchAwait(notificationCompleteLatch);
        verifyLatchAwait(shutdownCompleteLatch, never());
        verify(worker, never()).shutdown();
    }

    @Test
    public void testShutdownInterrupted() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

        when(shutdownCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
        when(shutdownCompleteLatch.getCount()).thenReturn(1L);

        assertThat(requestedShutdownCallable.call(), equalTo(false));
        verifyLatchAwait(notificationCompleteLatch);
        verifyLatchAwait(shutdownCompleteLatch);
        verify(worker).shutdown();
    }

    @Test
    public void testInterruptedAfterNotification() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return true;
        });

        assertThat(requestedShutdownCallable.call(), equalTo(false));
        verifyLatchAwait(notificationCompleteLatch);
        verifyLatchAwait(shutdownCompleteLatch, never());
        verify(worker, never()).shutdown();
    }

    @Test
    public void testInterruptedAfterWorkerShutdown() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return true;
        }).when(worker).shutdown();

        assertThat(requestedShutdownCallable.call(), equalTo(false));
        verifyLatchAwait(notificationCompleteLatch);
        verifyLatchAwait(shutdownCompleteLatch, never());
        verify(worker).shutdown();
    }

    @Test
    public void testInterruptedDuringNotification() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return false;
        });
        when(notificationCompleteLatch.getCount()).thenReturn(1L);

        when(shutdownCompleteLatch.getCount()).thenReturn(1L);

        assertThat(requestedShutdownCallable.call(), equalTo(false));
        verifyLatchAwait(notificationCompleteLatch);
        verify(notificationCompleteLatch).getCount();

        verifyLatchAwait(shutdownCompleteLatch, never());
        verify(shutdownCompleteLatch).getCount();

        verify(worker, never()).shutdown();
    }

    @Test
    public void testInterruptedDuringShutdown() throws Exception {
        Callable<Boolean> requestedShutdownCallable = buildRequestedShutdownCallable();

        when(notificationCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenReturn(true);

        when(shutdownCompleteLatch.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return false;
        });
        when(shutdownCompleteLatch.getCount()).thenReturn(1L);

        assertThat(requestedShutdownCallable.call(), equalTo(false));
        verifyLatchAwait(notificationCompleteLatch);
        verify(notificationCompleteLatch, never()).getCount();

        verifyLatchAwait(shutdownCompleteLatch);
        verify(shutdownCompleteLatch).getCount();

        verify(worker).shutdown();
    }

    @Test(expected = IllegalStateException.class)
    public void testWorkerShutdownCallableThrows() throws Exception {
        Callable<Boolean> requestedShutdownCallable = new GracefulShutdownCoordinator().createGracefulShutdownCallable(contextCallable);
        when(contextCallable.call()).thenThrow(new IllegalStateException("Bad Shutdown"));

        requestedShutdownCallable.call();
    }

    private void verifyLatchAwait(CountDownLatch latch) throws Exception {
        verifyLatchAwait(latch, times(1));
    }

    private void verifyLatchAwait(CountDownLatch latch, int times) throws Exception {
        verifyLatchAwait(latch, times(times));
    }

    private void verifyLatchAwait(CountDownLatch latch, VerificationMode verificationMode) throws Exception {
        verify(latch, verificationMode).await(anyLong(), any(TimeUnit.class));
    }

    private void mockLatchAwait(CountDownLatch latch, Boolean initial, Boolean... remaining) throws Exception {
        when(latch.await(anyLong(), any(TimeUnit.class))).thenReturn(initial, remaining);
    }

    private Callable<Boolean> buildRequestedShutdownCallable() throws Exception {
        GracefulShutdownContext context = new GracefulShutdownContext(shutdownCompleteLatch,
                notificationCompleteLatch, worker);
        when(contextCallable.call()).thenReturn(context);
        return new GracefulShutdownCoordinator().createGracefulShutdownCallable(contextCallable);
    }

    private void mockShardInfoConsumerMap(Integer initialItemCount, Integer... additionalItemCounts) {
        when(worker.getShardInfoShardConsumerMap()).thenReturn(shardInfoConsumerMap);
        Boolean additionalEmptyStates[] = new Boolean[additionalItemCounts.length];
        for (int i = 0; i < additionalItemCounts.length; ++i) {
            additionalEmptyStates[i] = additionalItemCounts[i] == 0;
        }
        when(shardInfoConsumerMap.size()).thenReturn(initialItemCount, additionalItemCounts);
        when(shardInfoConsumerMap.isEmpty()).thenReturn(initialItemCount == 0, additionalEmptyStates);
    }

}