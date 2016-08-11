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
package com.amazonaws.services.kinesis.clientlibrary.proxies.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisLocalFileProxy;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Temporary util class for generating data in a local file (used by KinesisLocalFileProxy).
 */
public class KinesisLocalFileDataCreator {

    /**
     * Partition key prefix - also referenced in KinesisLocalFileProxyTest.
     */
    public static final String PARTITION_KEY_PREFIX = "PK_";

    private static final String FILE_NAME_SUFFIX = ".dat";

    private static final long RAND_SEED_VALUE = 1092387456L;
    // Used to cap the size of the random "hole" in sequence numbers.
    private static final int NUM_BITS = 3;
    private static Random randomGenerator = new Random(RAND_SEED_VALUE);

    private static final int PARTITION_KEY_LENGTH = 10;
    private static final int DATA_LENGTH = 40;

    /**
     * Starting timestamp - also referenced in KinesisLocalFileProxyTest.
     */
    public static final long STARTING_TIMESTAMP = 1462345678910L;

    /**
     * This is used to allow few records to have the same timestamps (to mimic real life scenarios).
     * Records 5n-1 and 5n will have the same timestamp (n > 0).
     */
    private static final int DIVISOR = 5;

    private KinesisLocalFileDataCreator() {
    }

    /** Creates a temp file (in default temp file location) with fake Kinesis data records.
     * This method does not support resharding use cases.
     * @param numShards Number of shards
     * @param shardIdPrefix Prefix for shardIds (1, 2, ..., N will be added at the end to create shardIds)
     * @param numRecordsPerShard Number of records to generate per shard
     * @param startingSequenceNumber Sequence numbers in the generated data will be >= this number
     * @param fileNamePrefix Prefix of the filename
     * @return File created with the fake Kinesis records.
     * @throws IOException Thrown if there are issues creating the file.
     */
    public static File generateTempDataFile(
            int numShards,
            String shardIdPrefix,
            int numRecordsPerShard,
            BigInteger startingSequenceNumber,
            String fileNamePrefix)
        throws IOException {
        List<Shard> shardList = createShardList(numShards, shardIdPrefix, startingSequenceNumber);
        return generateTempDataFile(shardList, numRecordsPerShard, fileNamePrefix);
    }

    /**
     * Creates a temp file (in default temp file location) with fake Kinesis data records.
     * Records will be put in all shards.
     * @param fileNamePrefix Prefix for the name of the temp file
     * @param shardList List of shards (we use the shardId and sequenceNumberRange fields)
     * @param numRecordsPerShard Num records per shard (the shard sequenceNumberRange should be large enough
     *     for us to allow these many records with some "holes")
     * @return File with stream data filled in
     * @throws IOException Thrown if there are issues creating/updating the file
     */
    public static File generateTempDataFile(List<Shard> shardList, int numRecordsPerShard, String fileNamePrefix)
        throws IOException {
        File file = File.createTempFile(fileNamePrefix, FILE_NAME_SUFFIX);
        try (BufferedWriter fileWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            ObjectMapper objectMapper = new ObjectMapper();
            String serializedShardList =
                    objectMapper.writeValueAsString(new KinesisLocalFileProxy.SerializedShardList(shardList));
            fileWriter.write(serializedShardList);
            fileWriter.newLine();
            BigInteger sequenceNumberIncrement = new BigInteger("0");
            long timestamp = STARTING_TIMESTAMP;
            for (int i = 0; i < numRecordsPerShard; i++) {
                for (Shard shard : shardList) {
                    BigInteger sequenceNumber =
                            new BigInteger(shard.getSequenceNumberRange().getStartingSequenceNumber()).add(
                                    sequenceNumberIncrement);
                    String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
                    BigInteger maxSequenceNumber = KinesisLocalFileProxy.MAX_SEQUENCE_NUMBER;
                    if (endingSequenceNumber != null) {
                        maxSequenceNumber = new BigInteger(endingSequenceNumber);
                    }
                    if (maxSequenceNumber.compareTo(sequenceNumber) != 1) {
                        throw new IllegalArgumentException("Not enough space in shard");
                    }
                    String partitionKey =
                            PARTITION_KEY_PREFIX + shard.getShardId() + generateRandomString(PARTITION_KEY_LENGTH);
                    String data = generateRandomString(DATA_LENGTH);

                    // Allow few records to have the same timestamps (to mimic real life scenarios).
                    timestamp = (i % DIVISOR == 0) ? timestamp : timestamp + 1;
                    String line = shard.getShardId() + "," + sequenceNumber + "," + partitionKey + "," + data + ","
                            + timestamp;

                    fileWriter.write(line);
                    fileWriter.newLine();
                    sequenceNumberIncrement = sequenceNumberIncrement.add(BigInteger.ONE);
                    sequenceNumberIncrement = sequenceNumberIncrement.add(new BigInteger(NUM_BITS, randomGenerator));
                }
            }
        }
        return file;
    }

    /** Helper method to create a list of shards (which can then be used to generate data files).
     * @param numShards Number of shards
     * @param shardIdPrefix Prefix for the shardIds
     * @param startingSequenceNumber Starting sequence number for all the shards
     * @return List of shards (with no reshard events).
     */
    public static List<Shard> createShardList(int numShards, String shardIdPrefix, BigInteger startingSequenceNumber) {
        List<Shard> shards = new ArrayList<Shard>(numShards);

        SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
        sequenceNumberRange.setStartingSequenceNumber(startingSequenceNumber.toString());
        sequenceNumberRange.setEndingSequenceNumber(null);
        BigInteger perShardHashKeyRange =
                KinesisLocalFileProxy.MAX_HASHKEY_VALUE.divide(new BigInteger(Integer.toString(numShards)));
        BigInteger hashKeyRangeStart = new BigInteger("0");
        for (int i = 0; i < numShards; i++) {
            Shard shard = new Shard();
            shard.setShardId(shardIdPrefix + i);
            shard.setSequenceNumberRange(sequenceNumberRange);
            BigInteger hashKeyRangeEnd = hashKeyRangeStart.add(perShardHashKeyRange);
            HashKeyRange hashKeyRange = new HashKeyRange();
            hashKeyRange.setStartingHashKey(hashKeyRangeStart.toString());
            hashKeyRange.setEndingHashKey(hashKeyRangeEnd.toString());
            shards.add(shard);
        }

        return shards;
    }

    /** Generates a random string of specified length.
     * @param length String of length will be generated
     * @return Random generated string
     */
    private static String generateRandomString(int length) {
        StringBuffer str = new StringBuffer();
        final int startingCharAsciiValue = 97;
        final int numChars = 26;
        for (int i = 0; i < length; i++) {
            str.append((char) (randomGenerator.nextInt(numChars - 1) + startingCharAsciiValue));
        }
        return str.toString();
    }

    /** Creates a new temp file populated with fake Kinesis data records.
     * @param args Expects 5 args: numShards, shardPrefix, numRecordsPerShard, startingSequenceNumber, fileNamePrefix
     */
    // CHECKSTYLE:OFF MagicNumber
    // CHECKSTYLE:IGNORE UncommentedMain FOR NEXT 2 LINES
    public static void main(String[] args) {
        int numShards = 1;
        String shardIdPrefix = "shardId";
        int numRecordsPerShard = 17;
        BigInteger startingSequenceNumber = new BigInteger("99");
        String fileNamePrefix = "kinesisFakeRecords";

        try {
            if ((args.length != 0) && (args.length != 5)) {
                // Temporary util code, so not providing detailed usage feedback.
                System.out.println("Unexpected number of arguments.");
                System.exit(0);
            }

            if (args.length == 5) {
                numShards = Integer.parseInt(args[0]);
                shardIdPrefix = args[1];
                numRecordsPerShard = Integer.parseInt(args[2]);
                startingSequenceNumber = new BigInteger(args[3]);
                fileNamePrefix = args[4];
            }

            File file = KinesisLocalFileDataCreator.generateTempDataFile(
                    numShards,
                    shardIdPrefix,
                    numRecordsPerShard,
                    startingSequenceNumber,
                    fileNamePrefix);
            System.out.println("Created fake kinesis records in file: " + file.getAbsolutePath());
        } catch (Exception e) {
            // CHECKSTYLE:IGNORE IllegalCatch FOR NEXT -1 LINES
            System.out.println("Caught Exception: " + e);
        }

    }
    // CHECKSTYLE:ON MagicNumber

}
