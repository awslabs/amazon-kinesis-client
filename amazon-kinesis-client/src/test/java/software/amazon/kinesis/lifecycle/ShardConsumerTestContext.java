package software.amazon.kinesis.lifecycle;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.mockito.Mock;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput;
import software.amazon.kinesis.retrieval.RecordsPublisher;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

@Data
public class ShardConsumerTestContext {
    final String shardId = "shardId-0-0";
    final String concurrencyToken = "TestToken";
    ShardInfo shardInfo;
    TaskExecutionListenerInput initialTaskInput;
    TaskExecutionListenerInput processTaskInput;
    TaskExecutionListenerInput shutdownTaskInput;
    TaskExecutionListenerInput shutdownRequestedTaskInput;
    TaskExecutionListenerInput shutdownRequestedAwaitTaskInput;
    ExecutorService executorService;
    RecordsPublisher recordsPublisher = mock(RecordsPublisher.class);
    ShutdownNotification shutdownNotification = mock(ShutdownNotification.class);
    ConsumerState initialState = mock(ConsumerState.class);
    ConsumerTask initializeTask = mock(ConsumerTask.class);
    ConsumerState processingState = mock(ConsumerState.class);
    ConsumerTask processingTask = mock(ConsumerTask.class);
    ConsumerState shutdownState = mock(ConsumerState.class);
    ConsumerTask shutdownTask = mock(ConsumerTask.class);
    TaskResult initializeTaskResult = mock(TaskResult.class);
    TaskResult processingTaskResult = mock(TaskResult.class);
    ConsumerState shutdownCompleteState = mock(ConsumerState.class);
    ShardConsumerArgument shardConsumerArgument = mock(ShardConsumerArgument.class);
    ConsumerState shutdownRequestedState = mock(ConsumerState.class);
    ConsumerTask shutdownRequestedTask = mock(ConsumerTask.class);
    ConsumerState shutdownRequestedAwaitState = mock(ConsumerState.class);
    TaskExecutionListener taskExecutionListener = mock(TaskExecutionListener.class);
    ProcessRecordsInput processRecordsInput;
    Optional<Long> logWarningForTaskAfterMillis = Optional.empty();
    // Increased timeout to attempt to get auto-build working...
    static final int awaitTimeout = 5;
    static final TimeUnit awaitTimeoutUnit = TimeUnit.SECONDS;
}