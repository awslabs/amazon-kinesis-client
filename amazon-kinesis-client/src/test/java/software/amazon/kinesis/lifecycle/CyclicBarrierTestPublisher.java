package software.amazon.kinesis.lifecycle;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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
            requestBarrier.await(5, TimeUnit.SECONDS);
            return null;
        }).when(subscription).request(anyLong());
        doAnswer(a -> {
            if (enableCancelAwait) {
                requestBarrier.await(5, TimeUnit.SECONDS);
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
            subscriptionBarrier.await(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void restartFrom(RecordsRetrieved recordsRetrieved) {

    }

    public void awaitSubscription() throws InterruptedException, BrokenBarrierException, TimeoutException {
        subscriptionBarrier.await(5, TimeUnit.SECONDS);
        subscriptionBarrier.reset();
    }

    public void awaitRequest() throws InterruptedException, BrokenBarrierException, TimeoutException {
        requestBarrier.await(5, TimeUnit.SECONDS);
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


    public static void awaitBarrier(CyclicBarrier barrier) throws Exception {
        if (barrier != null) {
            barrier.await(5, TimeUnit.SECONDS);
        }
    }

    public static void awaitAndResetBarrier(CyclicBarrier barrier) throws Exception {
        if(barrier!=null) {
            barrier.await(5, TimeUnit.SECONDS);
            barrier.reset();
        }
    }
}
