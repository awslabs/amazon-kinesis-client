/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.retrieval;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.InvalidProtocolBufferException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.retrieval.kpl.Messages;

/**
 *
 */
@Slf4j
public class AggregatorUtil {
    public static final byte[] AGGREGATED_RECORD_MAGIC = new byte[]{-13, -119, -102, -62};
    private static final int DIGEST_SIZE = 16;
    private static final BigInteger STARTING_HASH_KEY = new BigInteger("0");
    // largest hash key = 2^128-1
    private static final BigInteger ENDING_HASH_KEY = new BigInteger(StringUtils.repeat("FF", 16), 16);

    /**
     * This method deaggregates the given list of Amazon Kinesis records into a
     * list of KPL user records. This method will then return the resulting list
     * of KPL user records.
     *
     * @param records A list of Amazon Kinesis records, each possibly aggregated.
     * @return A resulting list of deaggregated KPL user records.
     */
    public List<KinesisClientRecord> deaggregate(List<KinesisClientRecord> records) {
        return deaggregate(records, STARTING_HASH_KEY, ENDING_HASH_KEY);
    }

    /**
     * Deaggregate any KPL records found. This method converts the starting and ending hash keys to {@link BigInteger}s
     * before passing them on to {@link #deaggregate(List, BigInteger, BigInteger)}
     * 
     * @param records
     *            the records to potentially deaggreate
     * @param startingHashKey
     *            the starting hash key of the shard
     * @param endingHashKey
     *            the ending hash key of the shard
     * @return A list of records with any aggregate records deaggregated
     */
    public List<KinesisClientRecord> deaggregate(List<KinesisClientRecord> records, String startingHashKey,
            String endingHashKey) {
        return deaggregate(records, new BigInteger(startingHashKey), new BigInteger(endingHashKey));
    }

    /**
     * This method deaggregates the given list of Amazon Kinesis records into a
     * list of KPL user records. Any KPL user records whose explicit hash key or
     * partition key falls outside the range of the startingHashKey and the
     * endingHashKey are discarded from the resulting list. This method will
     * then return the resulting list of KPL user records.
     *
     * @param records         A list of Amazon Kinesis records, each possibly aggregated.
     * @param startingHashKey A BigInteger representing the starting hash key that the
     *                        explicit hash keys or partition keys of retained resulting KPL
     *                        user records must be greater than or equal to.
     * @param endingHashKey   A BigInteger representing the ending hash key that the the
     *                        explicit hash keys or partition keys of retained resulting KPL
     *                        user records must be smaller than or equal to.
     * @return A resulting list of KPL user records whose explicit hash keys or
     * partition keys fall within the range of the startingHashKey and
     * the endingHashKey.
     */
    // CHECKSTYLE:OFF NPathComplexity
    public List<KinesisClientRecord> deaggregate(List<KinesisClientRecord> records,
                                                        BigInteger startingHashKey,
                                                        BigInteger endingHashKey) {
        List<KinesisClientRecord> result = new ArrayList<>();
        byte[] magic = new byte[AGGREGATED_RECORD_MAGIC.length];
        byte[] digest = new byte[DIGEST_SIZE];

        for (KinesisClientRecord r : records) {
            boolean isAggregated = true;
            long subSeqNum = 0;
            ByteBuffer bb = r.data();

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
                byte[] calculatedDigest = calculateTailCheck(messageData);

                if (!Arrays.equals(digest, calculatedDigest)) {
                    isAggregated = false;
                } else {
                    try {
                        Messages.AggregatedRecord ar = Messages.AggregatedRecord.parseFrom(messageData);
                        List<String> pks = ar.getPartitionKeyTableList();
                        List<String> ehks = ar.getExplicitHashKeyTableList();
                        long aat = r.approximateArrivalTimestamp() == null
                                ? -1 : r.approximateArrivalTimestamp().toEpochMilli();
                        try {
                            int recordsInCurrRecord = 0;
                            for (Messages.Record mr : ar.getRecordsList()) {
                                String explicitHashKey = null;
                                String partitionKey = pks.get((int) mr.getPartitionKeyIndex());
                                if (mr.hasExplicitHashKeyIndex()) {
                                    explicitHashKey = ehks.get((int) mr.getExplicitHashKeyIndex());
                                }

                                BigInteger effectiveHashKey = effectiveHashKey(partitionKey, explicitHashKey);

                                if (effectiveHashKey.compareTo(startingHashKey) < 0
                                        || effectiveHashKey.compareTo(endingHashKey) > 0) {
                                    for (int toRemove = 0; toRemove < recordsInCurrRecord; ++toRemove) {
                                        result.remove(result.size() - 1);
                                    }
                                    break;
                                }

                                ++recordsInCurrRecord;

                                KinesisClientRecord record = r.toBuilder()
                                        .data(ByteBuffer.wrap(mr.getData().toByteArray()))
                                        .partitionKey(partitionKey)
                                        .explicitHashKey(explicitHashKey)
                                        .build();
                                result.add(convertRecordToKinesisClientRecord(record, true, subSeqNum++, explicitHashKey));
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
                            sb.append("Sequence number: ").append(r.sequenceNumber()).append("\n")
                                    .append("Raw data: ")
                                    .append(javax.xml.bind.DatatypeConverter.printBase64Binary(messageData)).append("\n");
                            log.error(sb.toString(), e);
                        }
                    } catch (InvalidProtocolBufferException e) {
                        isAggregated = false;
                    }
                }
            }

            if (!isAggregated) {
                bb.rewind();
                result.add(r);
            }
        }
        return result;
    }

    protected byte[] calculateTailCheck(byte[] data) {
        return md5(data);
    }

    protected BigInteger effectiveHashKey(String partitionKey, String explicitHashKey) throws UnsupportedEncodingException {
        if (explicitHashKey == null) {
            return new BigInteger(1, md5(partitionKey.getBytes("UTF-8")));
        }
        return new BigInteger(explicitHashKey);
    }

    private byte[] md5(byte[] data) {
        try {
            MessageDigest d = MessageDigest.getInstance("MD5");
            return d.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public KinesisClientRecord convertRecordToKinesisClientRecord(@NonNull final KinesisClientRecord record,
                                                                  final boolean aggregated,
                                                                  final long subSequenceNumber,
                                                                  final String explicitHashKey) {
        return KinesisClientRecord.builder()
                .data(record.data())
                .partitionKey(record.partitionKey())
                .approximateArrivalTimestamp(record.approximateArrivalTimestamp())
                .encryptionType(record.encryptionType())
                .sequenceNumber(record.sequenceNumber())
                .aggregated(aggregated)
                .subSequenceNumber(subSequenceNumber)
                .explicitHashKey(explicitHashKey)
                .build();
    }
}
