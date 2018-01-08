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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import com.amazonaws.services.kinesis.clientlibrary.proxies.util.KinesisLocalFileDataCreator;

/** Factory for KinesisProxy objects that use a local file for data. Useful for testing purposes.
 *
 */
public class KinesisLocalFileProxyFactory implements IKinesisProxyFactory {
    
    private static final int DEFAULT_NUM_SHARDS = 3;
    private static final String DEFAULT_SHARD_ID_PREFIX = "ShardId-";
    private static final int DEFAULT_NUM_RECORDS_PER_SHARD = 10;
    private static final BigInteger DEFAULT_STARTING_SEQUENCE_NUMBER = BigInteger.ZERO;
    
    private static final String DEFAULT_TEST_PROXY_FILE = "defaultKinesisProxyLocalFile";
    
    private IKinesisProxy testKinesisProxy;

    /** 
     * @param fileName File to be used for stream data.
     * If the file exists then it is expected to contain information for creating a test proxy object.
     * If the file does not exist then a temporary file containing default values for a test proxy object
     * will be created and used.

     * @throws IOException This will be thrown if we can't read/create the data file.
     */
    public KinesisLocalFileProxyFactory(String fileName) throws IOException {
        File f = new File(fileName);
        if (!f.exists()) {
            f = KinesisLocalFileDataCreator.generateTempDataFile(
                DEFAULT_NUM_SHARDS, DEFAULT_SHARD_ID_PREFIX, DEFAULT_NUM_RECORDS_PER_SHARD, 
                DEFAULT_STARTING_SEQUENCE_NUMBER, DEFAULT_TEST_PROXY_FILE);
        }
        testKinesisProxy = new KinesisLocalFileProxy(f.getAbsolutePath());
    }

    /* (non-Javadoc)
     * @see com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxyFactory#getProxy(java.lang.String)
     */
    @Override
    public IKinesisProxy getProxy(String streamARN) {
        return testKinesisProxy;
    }
}
