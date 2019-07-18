package software.amazon.kinesis.lifecycle;

import lombok.AllArgsConstructor;
import org.reactivestreams.Subscriber;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RecordsRetrievedAck;

import java.util.UUID;

@AllArgsConstructor
public class ShardConsumerNotifyingSubscriber implements NotifyingSubscriber {

    private final Subscriber<RecordsRetrieved> delegate;

    private final RecordsPublisher recordsPublisher;

    @Override
    public Subscriber<RecordsRetrieved> getDelegateSubscriber() {
        return delegate;
    }

    @Override
    public RecordsPublisher getWaitingRecordsPublisher() {
        return recordsPublisher;
    }

    @Override
    public RecordsRetrievedAck getRecordsRetrievedAck(RecordsRetrieved recordsRetrieved) {
        return new RecordsRetrievedAck() {
            @Override
            public String deliveredSequenceNumber() {
                return recordsRetrieved.batchSequenceNumber();
            }

            @Override
            public UUID batchUniqueIdentifier() {
                return recordsRetrieved.batchUniqueIdentifier();
            }
        };
    }
}
