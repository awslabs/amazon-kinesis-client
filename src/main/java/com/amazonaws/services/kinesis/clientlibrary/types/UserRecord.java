/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.kinesis.clientlibrary.types;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This class represents a KPL user record.
 */
@SuppressWarnings("serial")
public class UserRecord extends Record {
    private static final Log LOG = LogFactory.getLog(UserRecord.class);

    private static final byte[] AGGREGATED_RECORD_MAGIC = new byte[] {-13, -119, -102, -62 };
    private static final int DIGEST_SIZE = 16;
    private static final BigInteger SMALLEST_HASH_KEY = new BigInteger("0");
    // largest hash key = 2^128-1
    private static final BigInteger LARGEST_HASH_KEY = new BigInteger(StringUtils.repeat("FF", 16), 16);

    private final long subSequenceNumber;
    private final String explicitHashKey;
    private final boolean aggregated;

    /**
     * Create a User Record from a Kinesis Record.
     *
     * @param record Kinesis record
     */
    public UserRecord(Record record) {
        this(false, record, 0, null);
    }

    /**
     * Create a User Record.
     * 
     * @param aggregated whether the record is aggregated
     * @param record Kinesis record
     * @param subSequenceNumber subsequence number
     * @param explicitHashKey explicit hash key
     */
    protected UserRecord(boolean aggregated, Record record, long subSequenceNumber, String explicitHashKey) {
        if (subSequenceNumber < 0) {
            throw new IllegalArgumentException("Cannot have an invalid, negative subsequence number");
        }
        
        this.aggregated = aggregated;
        this.subSequenceNumber = subSequenceNumber;
        this.explicitHashKey = explicitHashKey;
        
        this.setSequenceNumber(record.getSequenceNumber());
        this.setData(record.getData());
        this.setPartitionKey(record.getPartitionKey());
        this.setApproximateArrivalTimestamp(record.getApproximateArrivalTimestamp());
    }

    /**
     * @return subSequenceNumber of this UserRecord.
     */
    public long getSubSequenceNumber() {
        return subSequenceNumber;
    }

    /**
     * @return explicitHashKey of this UserRecord.
     */
    public String getExplicitHashKey() {
        return explicitHashKey;
    }

    /**
     * @return a boolean indicating whether this UserRecord is aggregated.
     */
    public boolean isAggregated() {
        return aggregated;
    }

    /**
     * @return the String representation of this UserRecord.
     */
    @Override
    public String toString() {
        return "UserRecord [subSequenceNumber=" + subSequenceNumber + ", explicitHashKey=" + explicitHashKey
                + ", aggregated=" + aggregated + ", getSequenceNumber()=" + getSequenceNumber() + ", getData()="
                + getData() + ", getPartitionKey()=" + getPartitionKey() + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (aggregated ? 1231 : 1237);
        result = prime * result + ((explicitHashKey == null) ? 0 : explicitHashKey.hashCode());
        result = prime * result + (int) (subSequenceNumber ^ (subSequenceNumber >>> 32));
        return result;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UserRecord other = (UserRecord) obj;
        if (aggregated != other.aggregated) {
            return false;
        }
        if (explicitHashKey == null) {
            if (other.explicitHashKey != null) {
                return false;
            }
        } else if (!explicitHashKey.equals(other.explicitHashKey)) {
            return false;
        }
        if (subSequenceNumber != other.subSequenceNumber) {
            return false;
        }
        return true;
    }

    private static byte[] md5(byte[] data) {
        try {
            MessageDigest d = MessageDigest.getInstance("MD5");
            return d.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method deaggregates the given list of Amazon Kinesis records into a
     * list of KPL user records. This method will then return the resulting list
     * of KPL user records.
     * 
     * @param records
     *            A list of Amazon Kinesis records, each possibly aggregated.
     * @return A resulting list of deaggregated KPL user records.
     */
    public static List<UserRecord> deaggregate(List<Record> records) {
        return deaggregate(records, SMALLEST_HASH_KEY, LARGEST_HASH_KEY);
    }

    /**
     * This method deaggregates the given list of Amazon Kinesis records into a
     * list of KPL user records. Any KPL user records whose explicit hash key or
     * partition key falls outside the range of the startingHashKey and the
     * endingHashKey are discarded from the resulting list. This method will
     * then return the resulting list of KPL user records.
     * 
     * @param records
     *            A list of Amazon Kinesis records, each possibly aggregated.
     * @param startingHashKey
     *            A BigInteger representing the starting hash key that the
     *            explicit hash keys or partition keys of retained resulting KPL
     *            user records must be greater than or equal to.
     * @param endingHashKey
     *            A BigInteger representing the ending hash key that the the
     *            explicit hash keys or partition keys of retained resulting KPL
     *            user records must be smaller than or equal to.
     * @return A resulting list of KPL user records whose explicit hash keys or
     *          partition keys fall within the range of the startingHashKey and
     *          the endingHashKey.
     */
    // CHECKSTYLE:OFF NPathComplexity
    public static List<UserRecord> deaggregate(List<Record> records, BigInteger startingHashKey,
            BigInteger endingHashKey) {
        List<UserRecord> result = new ArrayList<>();
        byte[] magic = new byte[AGGREGATED_RECORD_MAGIC.length];
        byte[] digest = new byte[DIGEST_SIZE];

        for (Record r : records) {
            boolean isAggregated = true;
            long subSeqNum = 0;
            ByteBuffer bb = r.getData();

            if (bb.remaining() >= magic.length) {
                bb.get(magic);
            } else {
                isAggregated = false;
            }

            if (!Arrays.equals(AGGREGATED_RECORD_MAGIC, magic) || bb.remaining() <= DIGEST_SIZE) {
                isAggregated = false;
            }

            if (isAggregated) {
                int oldLimit = bb.limit();
                bb.limit(oldLimit - DIGEST_SIZE);
                byte[] messageData = new byte[bb.remaining()];
                bb.get(messageData);
                bb.limit(oldLimit);
                bb.get(digest);
                byte[] calculatedDigest = md5(messageData);

                if (!Arrays.equals(digest, calculatedDigest)) {
                    isAggregated = false;
                } else {
                    try {
                        Messages.AggregatedRecord ar = Messages.AggregatedRecord.parseFrom(messageData);
                        List<String> pks = ar.getPartitionKeyTableList();
                        List<String> ehks = ar.getExplicitHashKeyTableList();
                        long aat = r.getApproximateArrivalTimestamp() == null
                                ? -1 : r.getApproximateArrivalTimestamp().getTime();
                        try {
                            int recordsInCurrRecord = 0;
                            for (Messages.Record mr : ar.getRecordsList()) {
                                String explicitHashKey = null;
                                String partitionKey = pks.get((int) mr.getPartitionKeyIndex());
                                if (mr.hasExplicitHashKeyIndex()) {
                                    explicitHashKey = ehks.get((int) mr.getExplicitHashKeyIndex());
                                }

                                BigInteger effectiveHashKey = explicitHashKey != null
                                        ? new BigInteger(explicitHashKey)
                                        : new BigInteger(1, md5(partitionKey.getBytes("UTF-8")));

                                if (effectiveHashKey.compareTo(startingHashKey) < 0
                                        || effectiveHashKey.compareTo(endingHashKey) > 0) {
                                    for (int toRemove = 0; toRemove < recordsInCurrRecord; ++toRemove) {
                                        result.remove(result.size() - 1);
                                    }
                                    break;
                                }

                                ++recordsInCurrRecord;
                                Record record = new Record()
                                        .withData(ByteBuffer.wrap(mr.getData().toByteArray()))
                                        .withPartitionKey(partitionKey)
                                        .withSequenceNumber(r.getSequenceNumber())
                                        .withApproximateArrivalTimestamp(aat < 0 ? null : new Date(aat));
                                result.add(new UserRecord(true, record, subSeqNum++, explicitHashKey));
                            }
                        } catch (Exception e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("Unexpected exception during deaggregation, record was:\n");
                            sb.append("PKS:\n");
                            for (String s : pks) {
                                sb.append(s).append("\n");
                            }
                            sb.append("EHKS: \n");
                            for (String s : ehks) {
                                sb.append(s).append("\n");
                            }
                            for (Messages.Record mr : ar.getRecordsList()) {
                                sb.append("Record: [hasEhk=").append(mr.hasExplicitHashKeyIndex()).append(", ")
                                    .append("ehkIdx=").append(mr.getExplicitHashKeyIndex()).append(", ")
                                    .append("pkIdx=").append(mr.getPartitionKeyIndex()).append(", ")
                                    .append("dataLen=").append(mr.getData().toByteArray().length).append("]\n");
                            }
                            sb.append("Sequence number: ").append(r.getSequenceNumber()).append("\n")
                                .append("Raw data: ")
                                .append(javax.xml.bind.DatatypeConverter.printBase64Binary(messageData)).append("\n");
                            LOG.error(sb.toString(), e);
                        }
                    } catch (InvalidProtocolBufferException e) {
                        isAggregated = false;
                    }
                }
            }

            if (!isAggregated) {
                bb.rewind();
                result.add(new UserRecord(r));
            }
        }
        return result;
    }
    // CHECKSTYLE:ON NPathComplexity
}
