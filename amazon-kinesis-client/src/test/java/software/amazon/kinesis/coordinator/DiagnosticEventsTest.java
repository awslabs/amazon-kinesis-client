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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseBuilder;
import software.amazon.kinesis.leases.LeaseCoordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DiagnosticEventsTest {
    @Mock
    private ThreadPoolExecutor executor;

    @Mock
    private LeaseCoordinator leaseCoordinator;

    @Mock
    private DiagnosticEventHandler defaultHandler;

    private DiagnosticEventHandler customHandler = new CustomHandler();
    private boolean wasCustomHandlerInvoked;

    private final Throwable throwable = new TestRejectedTaskException();

    private final int activeThreadCount = 2;
    private final int corePoolSize = 4;
    private final int largestPoolSize = 8;
    private final int maximumPoolSize = 16;

    private SynchronousQueue<Runnable> executorQueue;
    private Collection<Lease> leaseAssignments;

    @Before
    public void setup() {
        wasCustomHandlerInvoked = false;

        executorQueue = new SynchronousQueue<>();

        final Lease lease = new LeaseBuilder().build();
        leaseAssignments = Collections.singletonList(lease);

        when(executor.getQueue()).thenReturn(executorQueue);
        when(executor.getActiveCount()).thenReturn(activeThreadCount);
        when(executor.getCorePoolSize()).thenReturn(corePoolSize);
        when(executor.getLargestPoolSize()).thenReturn(largestPoolSize);
        when(executor.getMaximumPoolSize()).thenReturn(maximumPoolSize);
        when(leaseCoordinator.getAssignments()).thenReturn(leaseAssignments);
    }

    @Test
    public void testExecutorStateEvent() {
        ExecutorStateEvent event = new ExecutorStateEvent(executor, leaseCoordinator);
        event.accept(defaultHandler);

        assertEquals(event.getActiveThreads(), activeThreadCount);
        assertEquals(event.getCoreThreads(), corePoolSize);
        assertEquals(event.getLargestPoolSize(), largestPoolSize);
        assertEquals(event.getMaximumPoolSize(), maximumPoolSize);
        assertEquals(event.getLeasesOwned(), leaseAssignments.size());
        assertEquals(0, event.getCurrentQueueSize());

        verify(defaultHandler, times(1)).visit(event);
    }

    @Test
    public void testExecutorStateEventWithCustomHandler() {
        ExecutorStateEvent event = new ExecutorStateEvent(executor, leaseCoordinator);
        event.accept(customHandler);

        assertTrue(wasCustomHandlerInvoked);
    }

    @Test
    public void testRejectedTaskEvent() {
        ExecutorStateEvent executorStateEvent = new ExecutorStateEvent(executor, leaseCoordinator);
        RejectedTaskEvent event = new RejectedTaskEvent(executorStateEvent, throwable);
        event.accept(defaultHandler);

        assertEquals(event.getExecutorStateEvent().getActiveThreads(), activeThreadCount);
        assertEquals(event.getExecutorStateEvent().getCoreThreads(), corePoolSize);
        assertEquals(event.getExecutorStateEvent().getLargestPoolSize(), largestPoolSize);
        assertEquals(event.getExecutorStateEvent().getMaximumPoolSize(), maximumPoolSize);
        assertEquals(event.getExecutorStateEvent().getLeasesOwned(), leaseAssignments.size());
        assertEquals(0, event.getExecutorStateEvent().getCurrentQueueSize());
        assertTrue(event.getThrowable() instanceof TestRejectedTaskException);

        verify(defaultHandler, times(1)).visit(event);
    }

    @Test
    public void testRejectedTaskEventWithCustomHandler() {
        ExecutorStateEvent executorStateEvent = new ExecutorStateEvent(executor, leaseCoordinator);
        RejectedTaskEvent event = new RejectedTaskEvent(executorStateEvent, throwable);
        customHandler = new CustomHandler();
        event.accept(customHandler);

        assertTrue(wasCustomHandlerInvoked);
    }

    @Test
    public void testDiagnosticEventFactory() {
        DiagnosticEventFactory factory = new DiagnosticEventFactory();

        ExecutorStateEvent executorStateEvent = factory.executorStateEvent(executor, leaseCoordinator);
        assertEquals(executorStateEvent.getActiveThreads(), activeThreadCount);
        assertEquals(executorStateEvent.getCoreThreads(), corePoolSize);
        assertEquals(executorStateEvent.getLargestPoolSize(), largestPoolSize);
        assertEquals(executorStateEvent.getMaximumPoolSize(), maximumPoolSize);
        assertEquals(executorStateEvent.getLeasesOwned(), leaseAssignments.size());
        assertEquals(0, executorStateEvent.getCurrentQueueSize());

        RejectedTaskEvent rejectedTaskEvent =
                factory.rejectedTaskEvent(executorStateEvent, new TestRejectedTaskException());
        assertEquals(rejectedTaskEvent.getExecutorStateEvent().getActiveThreads(), activeThreadCount);
        assertEquals(rejectedTaskEvent.getExecutorStateEvent().getCoreThreads(), corePoolSize);
        assertEquals(rejectedTaskEvent.getExecutorStateEvent().getLargestPoolSize(), largestPoolSize);
        assertEquals(rejectedTaskEvent.getExecutorStateEvent().getMaximumPoolSize(), maximumPoolSize);
        assertEquals(rejectedTaskEvent.getExecutorStateEvent().getLeasesOwned(), leaseAssignments.size());
        assertEquals(0, rejectedTaskEvent.getExecutorStateEvent().getCurrentQueueSize());
        assertTrue(rejectedTaskEvent.getThrowable() instanceof TestRejectedTaskException);
    }

    private class TestRejectedTaskException extends Exception {
        private TestRejectedTaskException() {
            super();
        }
    }

    private class CustomHandler implements DiagnosticEventHandler {
        @Override
        public void visit(ExecutorStateEvent event) {
            wasCustomHandlerInvoked = true;
        }

        @Override
        public void visit(RejectedTaskEvent event) {
            wasCustomHandlerInvoked = true;
        }
    }
}
