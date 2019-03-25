package software.amazon.kinesis.lifecycle;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.junit.rules.TestName;
import org.mockito.Mock;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Accessors(fluent = true)
public class CyclicBarrierShardConsumerContext {

    public CyclicBarrierShardConsumerContext(TestName testName){
        this.testName=testName;
        shardInfo = new ShardInfo(shardId, concurrencyToken, null, ExtendedSequenceNumber.TRIM_HORIZON);
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("test-" + testName.getMethodName() + "-%04d")
                .setDaemon(true).build();
        executorService = new ThreadPoolExecutor(4, 4, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), factory);

        processRecordsInput = ProcessRecordsInput.builder().isAtShardEnd(false).cacheEntryTime(Instant.now())
                .millisBehindLatest(1000L).records(Collections.emptyList()).build();
        initialTaskInput = TaskExecutionListenerInput.builder().shardInfo(shardInfo)
                .taskType(TaskType.INITIALIZE).build();
        processTaskInput = TaskExecutionListenerInput.builder().shardInfo(shardInfo)
                .taskType(TaskType.PROCESS).build();
        shutdownRequestedTaskInput = TaskExecutionListenerInput.builder().shardInfo(shardInfo)
                .taskType(TaskType.SHUTDOWN_NOTIFICATION).build();
        shutdownRequestedAwaitTaskInput = TaskExecutionListenerInput.builder().shardInfo(shardInfo)
                .taskType(TaskType.SHUTDOWN_COMPLETE).build();
        shutdownTaskInput = TaskExecutionListenerInput.builder().shardInfo(shardInfo)
                .taskType(TaskType.SHUTDOWN).build();
    }

    @Getter
    private TestName testName;
    @Getter
    private final String shardId = "shardId-0-0";
    @Getter
    private final String concurrencyToken = "TestToken";
    @Getter
    private ShardInfo shardInfo;
    @Getter
    @Setter
    private TaskExecutionListenerInput initialTaskInput;
    @Getter
    @Setter
    private TaskExecutionListenerInput processTaskInput;
    @Getter
    @Setter
    private TaskExecutionListenerInput shutdownTaskInput;
    @Getter
    @Setter
    private TaskExecutionListenerInput shutdownRequestedTaskInput;

    @Getter
    private final TaskExecutionListenerInput shutdownRequestedAwaitTaskInput;
    @Getter
    private final ExecutorService executorService;
    @Getter
    private final ProcessRecordsInput processRecordsInput;
    @Getter
    private final Optional<Long> logWarningForTaskAfterMillis = Optional.empty();

    @Getter
    private final RecordsPublisher recordsPublisher = mock(RecordsPublisher.class);
    @Getter
    private final ShutdownNotification shutdownNotification = mock(ShutdownNotification.class);
    @Getter
    private final ConsumerState initialState = mock(ConsumerState.class);
    @Getter
    private final ConsumerTask initializeTask = mock(ConsumerTask.class);
    @Getter
    private final ConsumerState processingState = mock (ConsumerState.class);
    @Getter
    private final ConsumerTask processingTask = mock(ConsumerTask.class);
    @Getter
    private final ConsumerState shutdownState = mock(ConsumerState.class);
    @Getter
    private final ConsumerTask shutdownTask = mock(ConsumerTask.class);
    @Getter
    private final TaskResult initializeTaskResult = mock (TaskResult.class);
    @Getter
    private final TaskResult processingTaskResult = mock (TaskResult.class);
    @Getter
    private final ConsumerState shutdownCompleteState = mock(ConsumerState.class);
    @Getter
    private final ShardConsumerArgument shardConsumerArgument = mock(ShardConsumerArgument.class);
    @Getter
    private final ConsumerState shutdownRequestedState = mock(ConsumerState.class);
    @Getter
    private final ConsumerTask shutdownRequestedTask = mock(ConsumerTask.class);
    @Getter
    private final ConsumerState shutdownRequestedAwaitState = mock(ConsumerState.class);
    @Getter
    private final TaskExecutionListener taskExecutionListener = mock(TaskExecutionListener.class);


    void mockSuccessfulShutdown(CyclicBarrier taskCallBarrier) {
        mockSuccessfulShutdown(taskCallBarrier, null);
    }

    void mockSuccessfulShutdown(CyclicBarrier taskArriveBarrier, CyclicBarrier taskDepartBarrier) {
        when(shutdownState().createTask(eq(shardConsumerArgument()), any(), any())).thenReturn(shutdownTask());
        when(shutdownState().taskType()).thenReturn(TaskType.SHUTDOWN);
        when(shutdownTask().taskType()).thenReturn(TaskType.SHUTDOWN);
        when(shutdownTask().call()).thenAnswer(i -> {
            CyclicBarrierTestPublisher.awaitBarrier(taskArriveBarrier);
            CyclicBarrierTestPublisher.awaitBarrier(taskDepartBarrier);
            return new TaskResult(null);
        });
        when(shutdownState().shutdownTransition(any())).thenReturn(shutdownCompleteState());
        when(shutdownState().state()).thenReturn(ConsumerStates.ShardConsumerState.SHUTTING_DOWN);

        when(shutdownCompleteState().isTerminal()).thenReturn(true);
    }

    void mockSuccessfulProcessing(CyclicBarrier taskCallBarrier) {
        mockSuccessfulProcessing(taskCallBarrier, null);
    }

    void mockSuccessfulProcessing(CyclicBarrier taskCallBarrier, CyclicBarrier taskInterlockBarrier) {
        when(processingState().createTask(eq(shardConsumerArgument()), any(), any())).thenReturn(processingTask());
        when(processingState().requiresDataAvailability()).thenReturn(true);
        when(processingState().taskType()).thenReturn(TaskType.PROCESS);
        when(processingTask().taskType()).thenReturn(TaskType.PROCESS);
        when(processingTask().call()).thenAnswer(i -> {
            CyclicBarrierTestPublisher.awaitBarrier(taskCallBarrier);
            CyclicBarrierTestPublisher.awaitBarrier(taskInterlockBarrier);
            return processingTaskResult();
        });
        when(processingTaskResult().getException()).thenReturn(null);
        when(processingState().successTransition()).thenReturn(processingState());
        when(processingState().shutdownTransition(any())).thenReturn(shutdownState());
        when(processingState().state()).thenReturn(ConsumerStates.ShardConsumerState.PROCESSING);
    }

    void mockSuccessfulInitialize(CyclicBarrier taskCallBarrier) {
        mockSuccessfulInitialize(taskCallBarrier, null);
    }

    void mockSuccessfulInitialize(CyclicBarrier taskCallBarrier, CyclicBarrier taskInterlockBarrier) {

        when(initialState().createTask(eq(shardConsumerArgument()), any(), any())).thenReturn(initializeTask());
        when(initialState().taskType()).thenReturn(TaskType.INITIALIZE);
        when(initializeTask().taskType()).thenReturn(TaskType.INITIALIZE);
        when(initializeTask().call()).thenAnswer(i -> {
            CyclicBarrierTestPublisher.awaitBarrier(taskCallBarrier);
            CyclicBarrierTestPublisher.awaitBarrier(taskInterlockBarrier);
            return initializeTaskResult();
        });
        when(initializeTaskResult().getException()).thenReturn(null);
        when(initialState().requiresDataAvailability()).thenReturn(false);
        when(initialState().successTransition()).thenReturn(processingState());
        when(initialState().state()).thenReturn(ConsumerStates.ShardConsumerState.INITIALIZING);

    }
}
