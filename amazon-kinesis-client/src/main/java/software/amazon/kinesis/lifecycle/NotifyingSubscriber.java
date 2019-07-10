package software.amazon.kinesis.lifecycle;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RecordsRetrievedAck;

/**
 * Subscriber that notifies its publisher on receipt of the onNext event.
 */
public interface NotifyingSubscriber extends Subscriber<RecordsRetrieved> {

    /**
     * Return the actual subscriber to which the events needs to be delegated.
     * @return Subscriber<T> to be delegated
     */
    Subscriber<RecordsRetrieved> getDelegateSubscriber();

    /**
     * Return the publisher to be notified
     * @return RecordsPublisher to be notified.
     */
    RecordsPublisher getWaitingRecordsPublisher();

    /**
     * Construct RecordsRetrievedAck object from the incoming data and return it
     * @param t type of data
     * @return RecordsRetrievedAck
     */
    RecordsRetrievedAck getRecordsRetrievedAck(RecordsRetrieved recordsRetrieved);

    @Override
    default void onSubscribe(Subscription subscription) {
        getDelegateSubscriber().onSubscribe(subscription);
    }

    @Override
    default void onNext(RecordsRetrieved recordsRetrieved) {
        getWaitingRecordsPublisher().notify(getRecordsRetrievedAck(recordsRetrieved));
        getDelegateSubscriber().onNext(recordsRetrieved);
    }

    @Override
    default void onError(Throwable throwable) {
        getDelegateSubscriber().onError(throwable);
    }

    @Override
    default void onComplete() {
        getDelegateSubscriber().onComplete();
    }
}
