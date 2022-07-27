package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.ChildShard;

import java.util.List;

public interface IDataFetcher {

    DataFetcherResult getRecords(int maxRecords);

    void initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream);

    void initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream);

    void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream);

    void restartIterator();

    boolean isShardEndReached();

    List<ChildShard> getChildShards();
}
