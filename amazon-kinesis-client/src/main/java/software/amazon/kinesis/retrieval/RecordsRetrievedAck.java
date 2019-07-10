package software.amazon.kinesis.retrieval;

public interface RecordsRetrievedAck {

    /**
     * Sequence Number of the record batch that was delivered to the Subscriber/Observer.
     * @return deliveredSequenceNumber
     */
    String deliveredSequenceNumber();

}
