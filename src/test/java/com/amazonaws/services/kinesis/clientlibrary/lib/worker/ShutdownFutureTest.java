package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

@RunWith(MockitoJUnitRunner.class)
public class ShutdownFutureTest {

    @Mock
    private CountDownLatch shutdownCompleteLatch;
    @Mock
    private CountDownLatch notificationCompleteLatch;
    @Mock
    private Worker worker;
    @Mock
    private ConcurrentMap<ShardInfo, ShardConsumer> shardInfoConsumerMap;

    @Test
    public void testSimpleGetAlreadyCompleted() throws Exception {
        ShutdownFuture future = new ShutdownFuture(shutdownCompleteLatch, notificationCompleteLatch, worker);

        mockNotificationComplete(true);
        mockShutdownComplete(true);

        future.get();

        verify(notificationCompleteLatch).await(anyLong(), any(TimeUnit.class));
        verify(worker).shutdown();
        verify(shutdownCompleteLatch).await(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testNotificationNotCompleted() throws Exception {
        ShutdownFuture future = new ShutdownFuture(shutdownCompleteLatch, notificationCompleteLatch, worker);

        mockNotificationComplete(false, true);
        mockShutdownComplete(true);

        when(worker.getShardInfoShardConsumerMap()).thenReturn(shardInfoConsumerMap);
        when(shardInfoConsumerMap.isEmpty()).thenReturn(false);
        when(worker.isShutdownComplete()).thenReturn(false);

        when(notificationCompleteLatch.getCount()).thenReturn(1L);
        when(shutdownCompleteLatch.getCount()).thenReturn(1L);

        expectedTimeoutException(future);

        verify(worker, never()).shutdown();

        awaitFuture(future);

        verify(notificationCompleteLatch).getCount();
        verifyLatchAwait(notificationCompleteLatch, 2);

        verify(shutdownCompleteLatch).getCount();
        verifyLatchAwait(shutdownCompleteLatch);

        verify(worker).shutdown();

    }

    @Test
    public void testShutdownNotCompleted() throws Exception {
        ShutdownFuture future = new ShutdownFuture(shutdownCompleteLatch, notificationCompleteLatch, worker);
        mockNotificationComplete(true);
        mockShutdownComplete(false, true);

        when(shutdownCompleteLatch.getCount()).thenReturn(1L);
        when(worker.isShutdownComplete()).thenReturn(false);

        mockShardInfoConsumerMap(1);

        expectedTimeoutException(future);
        verify(worker).shutdown();
        awaitFuture(future);

        verifyLatchAwait(notificationCompleteLatch, 2);
        verifyLatchAwait(shutdownCompleteLatch, 2);

        verify(worker).isShutdownComplete();
        verify(worker).getShardInfoShardConsumerMap();

    }

    @Test
    public void testShutdownNotCompleteButWorkerShutdown() throws Exception {
        ShutdownFuture future = create();

        mockNotificationComplete(true);
        mockShutdownComplete(false);

        when(shutdownCompleteLatch.getCount()).thenReturn(1L);
        when(worker.isShutdownComplete()).thenReturn(true);
        mockShardInfoConsumerMap(1);

        awaitFuture(future);
        verify(worker).shutdown();
        verifyLatchAwait(notificationCompleteLatch);
        verifyLatchAwait(shutdownCompleteLatch);

        verify(worker, times(2)).isShutdownComplete();
        verify(worker).getShardInfoShardConsumerMap();
        verify(shardInfoConsumerMap).size();
    }

    @Test
    public void testShutdownNotCompleteButShardConsumerEmpty() throws Exception {
        ShutdownFuture future = create();
        mockNotificationComplete(true);
        mockShutdownComplete(false);

        mockOutstanding(shutdownCompleteLatch, 1L);

        when(worker.isShutdownComplete()).thenReturn(false);
        mockShardInfoConsumerMap(0);

        awaitFuture(future);
        verify(worker).shutdown();
        verifyLatchAwait(notificationCompleteLatch);
        verifyLatchAwait(shutdownCompleteLatch);

        verify(worker, times(2)).isShutdownComplete();
        verify(worker, times(2)).getShardInfoShardConsumerMap();

        verify(shardInfoConsumerMap).isEmpty();
        verify(shardInfoConsumerMap).size();
    }

    @Test
    public void testNotificationNotCompleteButShardConsumerEmpty() throws Exception {
        ShutdownFuture future = create();
        mockNotificationComplete(false);
        mockShutdownComplete(false);

        mockOutstanding(notificationCompleteLatch, 1L);
        mockOutstanding(shutdownCompleteLatch, 1L);

        when(worker.isShutdownComplete()).thenReturn(false);
        mockShardInfoConsumerMap(0);

        awaitFuture(future);
        verify(worker, never()).shutdown();
        verifyLatchAwait(notificationCompleteLatch);
        verify(shutdownCompleteLatch, never()).await();

        verify(worker, times(2)).isShutdownComplete();
        verify(worker, times(2)).getShardInfoShardConsumerMap();

        verify(shardInfoConsumerMap).isEmpty();
        verify(shardInfoConsumerMap).size();
    }

    @Test(expected = TimeoutException.class)
    public void testTimeExceededException() throws Exception {
        ShutdownFuture future = create();
        mockNotificationComplete(false);
        mockOutstanding(notificationCompleteLatch, 1L);
        when(worker.isShutdownComplete()).thenReturn(false);
        mockShardInfoConsumerMap(1);

        future.get(1, TimeUnit.NANOSECONDS);
    }

    private ShutdownFuture create() {
        return new ShutdownFuture(shutdownCompleteLatch, notificationCompleteLatch, worker);
    }

    private void mockShardInfoConsumerMap(Integer initialItemCount, Integer ... additionalItemCounts) {
        when(worker.getShardInfoShardConsumerMap()).thenReturn(shardInfoConsumerMap);
        Boolean additionalEmptyStates[] = new Boolean[additionalItemCounts.length];
        for(int i = 0; i < additionalItemCounts.length; ++i) {
            additionalEmptyStates[i] = additionalItemCounts[i] == 0;
        }
        when(shardInfoConsumerMap.size()).thenReturn(initialItemCount, additionalItemCounts);
        when(shardInfoConsumerMap.isEmpty()).thenReturn(initialItemCount == 0, additionalEmptyStates);
    }

    private void verifyLatchAwait(CountDownLatch latch) throws Exception {
        verifyLatchAwait(latch, 1);
    }

    private void verifyLatchAwait(CountDownLatch latch, int times) throws Exception {
        verify(latch, times(times)).await(anyLong(), any(TimeUnit.class));
    }

    private void expectedTimeoutException(ShutdownFuture future) throws Exception {
        boolean gotTimeout = false;
        try {
            awaitFuture(future);
        } catch (TimeoutException te) {
            gotTimeout = true;
        }
        assertThat("Expected a timeout exception to occur", gotTimeout);
    }

    private void awaitFuture(ShutdownFuture future) throws Exception {
        future.get(1, TimeUnit.SECONDS);
    }

    private void mockNotificationComplete(Boolean initial, Boolean... states) throws Exception {
        mockLatch(notificationCompleteLatch, initial, states);

    }

    private void mockShutdownComplete(Boolean initial, Boolean... states) throws Exception {
        mockLatch(shutdownCompleteLatch, initial, states);
    }

    private void mockLatch(CountDownLatch latch, Boolean initial, Boolean... states) throws Exception {
        when(latch.await(anyLong(), any(TimeUnit.class))).thenReturn(initial, states);
    }

    private void mockOutstanding(CountDownLatch latch, Long remaining, Long ... additionalRemaining) throws Exception {
        when(latch.getCount()).thenReturn(remaining, additionalRemaining);
    }

}