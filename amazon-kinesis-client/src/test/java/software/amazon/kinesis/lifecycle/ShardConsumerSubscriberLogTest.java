package software.amazon.kinesis.lifecycle;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class ShardConsumerSubscriberLogTest {

    private static final int awaitTimeout = 15;
    private static final TimeUnit awaitTimeoutUnit = TimeUnit.SECONDS;
    private CyclicBarrierShardConsumerContext testContext;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        testContext = new CyclicBarrierShardConsumerContext(testName);
    }

    @After
    public void after() {
        List<Runnable> remainder = testContext.executorService().shutdownNow();
        assertThat(remainder.isEmpty(), equalTo(true));
    }

    /**
     * Test to validate the warning message from ShardConsumer is not suppressed with the default configuration of 0
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterTimeoutIgnoreDefaultHappyPath() throws Exception {
        Exception exceptionToThrow=new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = {false, false, false, false, false};
        int[] expectedLogs={0,0,0,0,0};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,0, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is not suppressed with the default configuration of 0
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterTimeoutIgnoreDefault() throws Exception {
        Exception exceptionToThrow=new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = {false, false, true, false, true};
        int[] expectedLogs={0,0,1,1,2};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,0, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully supressed if we only have intermittant readTimeouts.
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterTimeoutIgnore1() throws Exception {
        Exception exceptionToThrow=new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = {false, false, true, false, true};
        int[] expectedLogs={0,0,0,0,0};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,1, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully logged if multiple sequential timeouts occur.
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterMultipleTimeoutIgnore1() throws Exception {
        Exception exceptionToThrow=new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = {true, true, false, true, true};
        int[] expectedLogs={0,1,1,1,2};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,1, exceptionToThrow);
    }

    /**
     * Test to validate the warning message from ShardConsumer is successfully logged if sequential timeouts occur.
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterMultipleTimeoutIgnore2() throws Exception {
        Exception exceptionToThrow=new software.amazon.kinesis.retrieval.RetryableRetrievalException("ReadTimeout");
        boolean[] requestsToThrowException = {true, true, true, true, true};
        int[] expectedLogs={0,0,1,2,3};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,2, exceptionToThrow);
    }

    /**
     * Test to validate the non-timeout warning message from ShardConsumer is not suppressed with the default configuration of 0
     * @throws Exception
     */
    @Test
    public void testLoggingSuppressedAfterExceptionDefault() throws Exception {
        //We're not throwing a ReadTimeout, so no suppression is expected.
        Exception exceptionToThrow=new RuntimeException("Uh oh Not a ReadTimeout");
        boolean[] requestsToThrowException = {false, false, true, false, true};
        int[] expectedLogs={0,0,1,1,2};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,0, exceptionToThrow);
    }

    /**
     * Test to validate the non-timeout warning message from ShardConsumer is not suppressed with 2 ReadTimeouts to ignore
     * @throws Exception
     */
    @Test
    public void testLoggingNotSuppressedAfterExceptionIgnore2ReadTimeouts() throws Exception {
        //We're not throwing a ReadTimeout, so no suppression is expected.
        Exception exceptionToThrow=new RuntimeException("Uh oh Not a ReadTimeout");
        boolean[] requestsToThrowException = {false, false, true, false, true};
        int[] expectedLogs={0,0,1,1,2};
        runLogSuppressionTest(requestsToThrowException, expectedLogs,2, exceptionToThrow);
    }

    /**
     * Runs the log suppression test which mocks exceptions to be thrown during shard consumption and validates log messages and requests recieved.
     * @param requestsToThrowException - Controls the test execution for how many requests to mock, and if they are successful, or throw an exception.
     *                                 true-> publish throws exception
     *                                 false-> publish successfully processes
     * @param expectedLogCounts - The expected warning log counts given the request profile from <tt>requestsToThrowException</tt> and <tt>readTimeoutsToIgnoreBeforeWarning</tt>
     * @param readTimeoutsToIgnoreBeforeWarning - Used to configure the ShardConsumer for the test to specify the configurable number of timeouts to suppress. This should not suppress any non-timeout exception.
     * @param exceptionToThrow - Specifies the type of exception to throw.
     * @throws Exception
     */
    private void runLogSuppressionTest(boolean[] requestsToThrowException, int[] expectedLogCounts, int readTimeoutsToIgnoreBeforeWarning, Exception exceptionToThrow) throws Exception {
        //Setup Test
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CyclicBarrier taskCallBarrier = new CyclicBarrier(2);

        testContext.mockSuccessfulInitialize(null);
        testContext.mockSuccessfulProcessing(taskCallBarrier);
        testContext.mockSuccessfulShutdown(null);
        CyclicBarrierTestPublisher cache = new CyclicBarrierTestPublisher(true, testContext.processRecordsInput() ,requestsToThrowException, exceptionToThrow);

        //Logging supressions specific setup
        int expectedRequest=0;
        int expectedPublish=0;

        ShardConsumer consumer = new ShardConsumer(cache, testContext.executorService(), testContext.shardInfo(), testContext.logWarningForTaskAfterMillis(),
                testContext.shardConsumerArgument(), testContext.initialState(), Function.identity(), 1, testContext.taskExecutionListener(),
                readTimeoutsToIgnoreBeforeWarning);

        Logger mockLogger = mock(Logger.class);
        injectLogger(consumer.subscriber(), mockLogger);

        //This needs to be executed in a seperate thread before an expected timeout
        // publish call to await the required cyclic barriers
        Runnable awaitingCacheThread = () -> {
            try {
                cache.awaitRequest();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        //Run the configured test
        boolean initComplete = false;
        while (!initComplete) {
            initComplete = consumer.initializeComplete().get();
        }
        //Initialize Shard Consumer Subscriptions
        consumer.subscribe();
        cache.awaitInitialSetup();
        for(int i=0; i< requestsToThrowException.length; i++){
            boolean shouldTimeout = requestsToThrowException[i];
            int expectedLogCount = expectedLogCounts[i];
            expectedRequest++;
            if(shouldTimeout){
                //Mock a ReadTimeout call
                executor.submit(awaitingCacheThread);
                cache.publish();
                // Sleep to increase liklihood of async processing is picked up in ShardConsumer.
                // Previous cyclic barriers are used to sync Publisher with the test, this test would require another subscriptionBarrier
                // in the ShardConsumer to fully sync the processing with the Test.
                Thread.sleep(50);
                //Restart subscription after failed request
                consumer.subscribe();
                cache.awaitSubscription();
            }else{
                expectedPublish++;
                //Mock a successful call
                cache.publish();
                awaitAndResetBarrier(taskCallBarrier);
                cache.awaitRequest();
            }
            assertEquals(expectedPublish,cache.getPublishCount());
            assertEquals(expectedRequest, cache.getRequestCount());
            if(exceptionToThrow instanceof RetryableRetrievalException
                    && exceptionToThrow.getMessage().contains("ReadTimeout")){
                verify(mockLogger, times(expectedLogCount)).warn(eq(
                        "{}: onError().  Cancelling subscription, and marking self as failed. KCL will" +
                                " recreate the subscription as neccessary to continue processing. If you " +
                                "are seeing this warning frequently consider increasing the SDK timeouts " +
                                "by providing an OverrideConfiguration to the kinesis client. Alternatively you" +
                                "can configure LifecycleConfig.readTimeoutsToIgnoreBeforeWarning to suppress" +
                                "intermittant ReadTimeout warnings."), anyString(), any());
            }else {
                verify(mockLogger, times(expectedLogCount)).warn(eq(
                        "{}: onError().  Cancelling subscription, and marking self as failed. KCL will " +
                                "recreate the subscription as neccessary to continue processing."), anyString(), any());
            }
        }

        //Clean Up Test
        injectLogger(consumer.subscriber(), LoggerFactory.getLogger(ShardConsumerSubscriber.class));

        Thread closingThread =
                new Thread(
                        new Runnable() {
                            public void run() {
                                consumer.leaseLost();
                            }
                        });
        closingThread.start();
        cache.awaitRequest();

        //We need to await and reset the task subscriptionBarrier prior to going into the shutdown loop.
        Runnable awaitingTaskThread = ()->{
            try {
                awaitAndResetBarrier(taskCallBarrier);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        executor.submit(awaitingTaskThread);
        while (!consumer.shutdownComplete().get()) { }
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
        //Make the logger non-final
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        // Inject the mock...
        field.set(subscriber, logger );

    }

    public static void awaitBarrier(CyclicBarrier barrier) throws Exception {
        if (barrier != null) {
            barrier.await(awaitTimeout, awaitTimeoutUnit);
        }
    }

    public static void awaitAndResetBarrier(CyclicBarrier barrier) throws Exception {
        if(barrier!=null) {
            barrier.await(awaitTimeout, awaitTimeoutUnit);
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

        private AtomicInteger requestCount=new AtomicInteger(0);

        Subscriber<? super RecordsRetrieved> subscriber;
        final Subscription subscription = mock(Subscription.class);
        private int publishCount=0;
        private ProcessRecordsInput processRecordsInput;

        CyclicBarrierTestPublisher(ProcessRecordsInput processRecordsInput) {
            this(false,processRecordsInput);
        }

        CyclicBarrierTestPublisher(boolean enableCancelAwait,ProcessRecordsInput processRecordsInput) { this(enableCancelAwait,processRecordsInput, null,null);}

        CyclicBarrierTestPublisher(boolean enableCancelAwait, ProcessRecordsInput processRecordsInput, boolean[] requestsToThrowException, Exception exceptionToThrow){
            doAnswer(a -> {
                requestBarrier.await(awaitTimeout, awaitTimeoutUnit);
                return null;
            }).when(subscription).request(anyLong());
            doAnswer(a -> {
                if (enableCancelAwait) {
                    requestBarrier.await(awaitTimeout, awaitTimeoutUnit);
                }
                return null;
            }).when(subscription).cancel();
            this.requestsToThrowException = requestsToThrowException;
            this.exceptionToThrow=exceptionToThrow;
            this.processRecordsInput=processRecordsInput;
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
                subscriptionBarrier.await(awaitTimeout, awaitTimeoutUnit);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void restartFrom(RecordsRetrieved recordsRetrieved) {

        }

        public void awaitSubscription() throws InterruptedException, BrokenBarrierException, TimeoutException {
            subscriptionBarrier.await(awaitTimeout, awaitTimeoutUnit);
            subscriptionBarrier.reset();
        }

        public void awaitRequest() throws InterruptedException, BrokenBarrierException, TimeoutException {
            requestBarrier.await(awaitTimeout, awaitTimeoutUnit);
            requestBarrier.reset();
        }

        public void awaitInitialSetup() throws InterruptedException, BrokenBarrierException, TimeoutException {
            awaitRequest();
            awaitSubscription();
        }

        public void publish() {
            if (requestsToThrowException != null && requestsToThrowException[requestCount.getAndIncrement() % requestsToThrowException.length]) {
                subscriber.onError(exceptionToThrow);
            } else {
                publish(()->processRecordsInput);
            }
        }

        public void publish(RecordsRetrieved input) {
            subscriber.onNext(input);
            publishCount++;
        }
    }

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
                awaitBarrier(taskArriveBarrier);
                awaitBarrier(taskDepartBarrier);
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
                awaitBarrier(taskCallBarrier);
                awaitBarrier(taskInterlockBarrier);
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
                awaitBarrier(taskCallBarrier);
                awaitBarrier(taskInterlockBarrier);
                return initializeTaskResult();
            });
            when(initializeTaskResult().getException()).thenReturn(null);
            when(initialState().requiresDataAvailability()).thenReturn(false);
            when(initialState().successTransition()).thenReturn(processingState());
            when(initialState().state()).thenReturn(ConsumerStates.ShardConsumerState.INITIALIZING);

        }
    }
}
