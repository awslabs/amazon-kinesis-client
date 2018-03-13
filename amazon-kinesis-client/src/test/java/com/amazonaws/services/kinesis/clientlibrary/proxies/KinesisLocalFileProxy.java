/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.proxies;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This is a (temporary) test utility class, to mimic Kinesis without having to integrate with Alpha.
 * In future, we should consider moving this to the Kinesis client/sampleApp package (if useful to
 * other Kinesis clients).
 */
public class KinesisLocalFileProxy implements IKinesisProxy {

    /**
     * Fields in the local file and their position in a line.
     */
    public enum LocalFileFields {
        /** Shard identifier. */
        SHARD_ID(0),
        /** Sequence number (assumed unique across shards. */
        SEQUENCE_NUMBER(1),
        /** Partition key associated with data record. */
        PARTITION_KEY(2),
        /** Data. */
        DATA(3),
        /** Approximate arrival timestamp. */
        APPROXIMATE_ARRIVAL_TIMESTAMP(4);

        private final int position;

        LocalFileFields(int position) {
            this.position = position;
        }

        /**
         * @return Position of the field in the line.
         */
        public int getPosition() {
            return position;
        }
    };

    private static final Log LOG = LogFactory.getLog(KinesisLocalFileProxy.class);

    private static final String ITERATOR_DELIMITER = ":";

    private static final int NUM_FIELDS_IN_FILE = LocalFileFields.values().length;

    private final Map<String, List<Record>> shardedDataRecords = new HashMap<String, List<Record>>();

    private List<Shard> shardList;

    // Ids of shards that are closed - used to return a null iterator in getRecords after the last record
    private Set<String> closedShards = new HashSet<String>();

    private static final int EXPONENT = 128;

    /**
     * Max value of the hashed partition key (2^128-1). Useful for constructing shards for a stream.
     */
    public static final BigInteger MAX_HASHKEY_VALUE = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE);

    /**
     * Max value of a sequence number (2^128 -1). Useful for defining sequence number range for a shard.
     */
    public static final BigInteger MAX_SEQUENCE_NUMBER = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE);

    /**
     * @param fileName File with data records (one per line).
     *        File format (shardId, sequenceNumber, partitionKey, dataRecord).
     * @throws IOException IOException
     */
    public KinesisLocalFileProxy(String fileName) throws IOException {
        super();
        populateDataRecordsFromFile(fileName);
    }

    private void populateDataRecordsFromFile(String file) throws IOException {
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            Charset charset = Charset.forName("UTF-8");
            CharsetEncoder encoder = charset.newEncoder();
            String str;
            str = in.readLine();
            if (str != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                SerializedShardList shards = objectMapper.readValue(str, SerializedShardList.class);
                shardList = shards.getShardList();
            }
            if (shardList == null) {
                shardList = new ArrayList<Shard>();
            }

            // Populate shardIds of shards that have an ending sequence number (and which != maxSeqNum).
            // GetRecords will return a null iterator for these after all data has been returned.
            for (Shard shard : shardList) {
                SequenceNumberRange range = shard.getSequenceNumberRange();
                if ((range != null) && (range.getEndingSequenceNumber() != null)) {
                    BigInteger endingSequenceNumber = new BigInteger(range.getEndingSequenceNumber());
                    if (endingSequenceNumber.compareTo(MAX_SEQUENCE_NUMBER) != 0) {
                        closedShards.add(shard.getShardId());
                    }
                }
                shardedDataRecords.put(shard.getShardId(), new ArrayList<Record>());
            }

            while ((str = in.readLine()) != null) {
                String[] strArr = str.split(",");
                if (strArr.length != NUM_FIELDS_IN_FILE) {
                    throw new InvalidArgumentException("Unexpected input in file."
                            + "Expected format (shardId, sequenceNumber, partitionKey, dataRecord, timestamp)");
                }
                String shardId = strArr[LocalFileFields.SHARD_ID.getPosition()];
                Record record = new Record();
                record.setSequenceNumber(strArr[LocalFileFields.SEQUENCE_NUMBER.getPosition()]);
                record.setPartitionKey(strArr[LocalFileFields.PARTITION_KEY.getPosition()]);
                ByteBuffer byteBuffer = encoder.encode(CharBuffer.wrap(strArr[LocalFileFields.DATA.getPosition()]));
                record.setData(byteBuffer);
                Date timestamp =
                        new Date(Long.parseLong(strArr[LocalFileFields.APPROXIMATE_ARRIVAL_TIMESTAMP.getPosition()]));
                record.setApproximateArrivalTimestamp(timestamp);
                List<Record> shardRecords = shardedDataRecords.get(shardId);
                if (shardRecords == null) {
                    shardRecords = new ArrayList<Record>();
                }
                shardRecords.add(record);
                shardedDataRecords.put(shardId, shardRecords);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy#getStreamInfo()
     */
    @Override
    public DescribeStreamResult getStreamInfo(String startShardId) throws ResourceNotFoundException {
        assert false : "getStreamInfo is not implemented.";
        return null;
    }

    @Override
    public Set<String> getAllShardIds() throws ResourceNotFoundException {
        Set<String> shardIds = new HashSet<String>();
        if (shardedDataRecords != null) {
            shardIds.addAll(shardedDataRecords.keySet());
        }

        return shardIds;
    }

    /**
     * Note, this method has package level access solely for testing purposes.
     */
    static String serializeIterator(String shardId, String sequenceNumber) {
        return String.format("%s%s%s", shardId, ITERATOR_DELIMITER, sequenceNumber);
    }

    /**
     * Container class for the return tuple of deserializeIterator.
     */
    // CHECKSTYLE:IGNORE VisibilityModifier FOR NEXT 10 LINES
    static class IteratorInfo {
        public String shardId;

        public String sequenceNumber;

        public IteratorInfo(String shardId, String sequenceNumber) {
            this.shardId = shardId;
            this.sequenceNumber = sequenceNumber;
        }
    }

    /**
     * Deserialize our iterator - used by test cases to inspect returned iterators.
     *
     * @param iterator
     * @return iteratorInfo
     */
    static IteratorInfo deserializeIterator(String iterator) {
        String[] splits = iterator.split(ITERATOR_DELIMITER);
        return new IteratorInfo(splits[0], splits[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, String iteratorEnum, String sequenceNumber)
        throws ResourceNotFoundException, InvalidArgumentException {
        /*
         * If we don't have records in this shard, any iterator will return the empty list. Using a
         * sequence number of 1 on an empty shard will give this behavior.
         */
        List<Record> shardRecords = shardedDataRecords.get(shardId);
        if (shardRecords == null) {
            throw new ResourceNotFoundException(shardId + " does not exist");
        }
        if (shardRecords.isEmpty()) {
            return serializeIterator(shardId, "1");
        }

        if (ShardIteratorType.LATEST.toString().equals(iteratorEnum)) {
            /*
             * If we do have records, LATEST should return an iterator that can be used to read the
             * last record. Our iterators are inclusive for convenience.
             */
            Record last = shardRecords.get(shardRecords.size() - 1);
            return serializeIterator(shardId, last.getSequenceNumber());
        } else if (ShardIteratorType.TRIM_HORIZON.toString().equals(iteratorEnum)) {
            return serializeIterator(shardId, shardRecords.get(0).getSequenceNumber());
        } else if (ShardIteratorType.AT_SEQUENCE_NUMBER.toString().equals(iteratorEnum)) {
            return serializeIterator(shardId, sequenceNumber);
        } else if (ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString().equals(iteratorEnum)) {
            BigInteger num = new BigInteger(sequenceNumber);
            num = num.add(BigInteger.ONE);
            return serializeIterator(shardId, num.toString());
        } else {
            throw new IllegalArgumentException("IteratorEnum value was invalid: " + iteratorEnum);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, String iteratorEnum)
        throws ResourceNotFoundException, InvalidArgumentException {
        /*
         * If we don't have records in this shard, any iterator will return the empty list. Using a
         * sequence number of 1 on an empty shard will give this behavior.
         */
        List<Record> shardRecords = shardedDataRecords.get(shardId);
        if (shardRecords == null) {
            throw new ResourceNotFoundException(shardId + " does not exist");
        }
        if (shardRecords.isEmpty()) {
            return serializeIterator(shardId, "1");
        }

        final String serializedIterator;
        if (ShardIteratorType.LATEST.toString().equals(iteratorEnum)) {
            /*
             * If we do have records, LATEST should return an iterator that can be used to read the
             * last record. Our iterators are inclusive for convenience.
             */
            Record last = shardRecords.get(shardRecords.size() - 1);
            serializedIterator = serializeIterator(shardId, last.getSequenceNumber());
        } else if (ShardIteratorType.TRIM_HORIZON.toString().equals(iteratorEnum)) {
            serializedIterator = serializeIterator(shardId, shardRecords.get(0).getSequenceNumber());
        } else {
            throw new IllegalArgumentException("IteratorEnum value was invalid: " + iteratorEnum);
        }
        return serializedIterator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, Date timestamp)
        throws ResourceNotFoundException, InvalidArgumentException {
        /*
         * If we don't have records in this shard, any iterator will return the empty list. Using a
         * sequence number of 1 on an empty shard will give this behavior.
         */
        List<Record> shardRecords = shardedDataRecords.get(shardId);
        if (shardRecords == null) {
            throw new ResourceNotFoundException(shardId + " does not exist");
        }
        if (shardRecords.isEmpty()) {
            return serializeIterator(shardId, "1");
        }

        final String serializedIterator;
        if (timestamp != null) {
            String seqNumAtTimestamp = findSequenceNumberAtTimestamp(shardRecords, timestamp);
            serializedIterator = serializeIterator(shardId, seqNumAtTimestamp);
        } else {
            throw new IllegalArgumentException("Timestamp must be specified for AT_TIMESTAMP iterator");
        }
        return serializedIterator;
    }

    private String findSequenceNumberAtTimestamp(final List<Record> shardRecords, final Date timestamp) {
        for (Record rec : shardRecords) {
            if (rec.getApproximateArrivalTimestamp().getTime() >= timestamp.getTime()) {
                return rec.getSequenceNumber();
            }
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy#get(java.nio.ByteBuffer, int)
     */
    @Override
    public GetRecordsResult get(String serializedKinesisIterator, int maxRecords)
        throws ResourceNotFoundException, InvalidArgumentException, ExpiredIteratorException {
        IteratorInfo iterator = deserializeIterator(serializedKinesisIterator);

        BigInteger startingPosition = new BigInteger(iterator.sequenceNumber);
        BigInteger lastRecordsSeqNo = BigInteger.ONE;
        List<Record> recordsToReturn = new ArrayList<Record>();
        List<Record> shardRecords = shardedDataRecords.get(iterator.shardId);
        if (shardRecords == null) {
            throw new ResourceNotFoundException(iterator.shardId + " does not exist");
        }

        boolean isHasMoreShards = false;

        for (int i = 0; i < shardRecords.size(); i++) {
            Record record = shardRecords.get(i);
            BigInteger recordSequenceNumber = new BigInteger(record.getSequenceNumber());
            // update lastRecordsSeqNo so if we return no records, it will be the seqNo of the last record.
            lastRecordsSeqNo = recordSequenceNumber;
            if (recordSequenceNumber.compareTo(startingPosition) >= 0) {
                // Set endIndex (of sublist) to cap at either maxRecords or end of list.
                int endIndex = Math.min(i + maxRecords, shardRecords.size());
                recordsToReturn.addAll(shardRecords.subList(i, endIndex));

                lastRecordsSeqNo = new BigInteger(shardRecords.get(endIndex - 1).getSequenceNumber());
                if (endIndex < shardRecords.size()) {
                    isHasMoreShards = true;
                }

                break;
            }
        }

        GetRecordsResult response = new GetRecordsResult();
        response.setRecords(recordsToReturn);

        // Set iterator only if the shard is not closed.
        if (isHasMoreShards || (!closedShards.contains(iterator.shardId))) {
            /*
             * Use the sequence number of the last record returned + 1 to compute the next iterator.
             */
            response.setNextShardIterator(serializeIterator(iterator.shardId, lastRecordsSeqNo.add(BigInteger.ONE)
                    .toString()));
            LOG.debug("Returning a non null iterator for shard " + iterator.shardId);
        } else {
            LOG.info("Returning null iterator for shard " + iterator.shardId);
        }

        return response;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PutRecordResult put(String exclusiveMinimumSequenceNumber,
            String explicitHashKey,
            String partitionKey,
            ByteBuffer data) throws ResourceNotFoundException, InvalidArgumentException {
        PutRecordResult output = new PutRecordResult();

        BigInteger startingPosition = BigInteger.ONE;

        if (exclusiveMinimumSequenceNumber != null) {
            startingPosition = new BigInteger(exclusiveMinimumSequenceNumber).add(BigInteger.ONE);
        }

        output.setSequenceNumber(startingPosition.toString());
        return output;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Shard> getShardList() throws ResourceNotFoundException {
        List<Shard> shards = new LinkedList<Shard>();
        shards.addAll(shardList);
        return shards;
    }

    /**
     * Used for serializing/deserializing the shard list to the file.
     */
    public static class SerializedShardList {

        private List<Shard> shardList = new LinkedList<Shard>();

        /**
         * Public to enable Jackson object mapper serialization.
         */
        public SerializedShardList() {
        }

        /**
         * @param shardList List of shards for the stream.
         */
        public SerializedShardList(List<Shard> shardList) {
            this.shardList.addAll(shardList);
        }

        /**
         * public to enable Jackson object mapper serialization.
         *
         * @return shardList
         */
        public List<Shard> getShardList() {
            return shardList;
        }

        /**
         * public to enable Jackson object mapper deserialization.
         *
         * @param shardList List of shards
         */
        public void setShardList(List<Shard> shardList) {
            this.shardList = shardList;
        }
    }
}
