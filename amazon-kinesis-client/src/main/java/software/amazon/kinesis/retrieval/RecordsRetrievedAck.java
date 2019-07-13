package software.amazon.kinesis.retrieval;

import java.util.UUID;

public interface RecordsRetrievedAck {

    /**
     * Sequence Number of the record batch that was delivered to the Subscriber/Observer.
     * @return deliveredSequenceNumber
     */
    String deliveredSequenceNumber();

    /**
     * Unique record batch identifier used to validate the ordering guarantees.
     * @return UUID
     */
    UUID batchUniqueIdentifier();

}
