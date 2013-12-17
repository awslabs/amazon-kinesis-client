/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

/**
 * Factory used for instantiating KinesisProxy objects (to fetch data from Kinesis).
 */
public class KinesisProxyFactory implements IKinesisProxyFactory {

    private final AWSCredentialsProvider credentialProvider;
    private static String defaultServiceName = "kinesis";
    private static String defaultRegionId = "us-east-1";
    private static final long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
    private static final int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;
    private final AmazonKinesisClient kinesisClient;
    private final long describeStreamBackoffTimeInMillis;
    private final int maxDescribeStreamRetryAttempts;

    /**
     * Constructor for creating a KinesisProxy factory, using the specified credentials provider and endpoint.
     * 
     * @param credentialProvider credentials provider used to sign requests
     * @param endpoint Amazon Kinesis endpoint to use
     */
    public KinesisProxyFactory(AWSCredentialsProvider credentialProvider, String endpoint) {
        this(credentialProvider, new ClientConfiguration(), endpoint, defaultServiceName, defaultRegionId,
                DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES);
    }

    /**
     * Constructor for KinesisProxy factory using the client configuration to use when interacting with Kinesis.
     * 
     * @param credentialProvider credentials provider used to sign requests
     * @param clientConfig Client Configuration used when instantiating an AmazonKinesisClient
     * @param endpoint Amazon Kinesis endpoint to use
     */
    public KinesisProxyFactory(AWSCredentialsProvider credentialProvider,
            ClientConfiguration clientConfig,
            String endpoint) {
        this(credentialProvider, clientConfig, endpoint, defaultServiceName, defaultRegionId,
                DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES);
        this.kinesisClient.setConfiguration(clientConfig);
    }

    /**
     * This constructor may be used to specify the AmazonKinesisClient to use.
     * 
     * @param credentialProvider credentials provider used to sign requests
     * @param client AmazonKinesisClient used to fetch data from Kinesis
     */
    public KinesisProxyFactory(AWSCredentialsProvider credentialProvider, AmazonKinesisClient client) {
        this(credentialProvider, client, DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES);
    }

    /**
     * Used internally and for development/testing.
     * 
     * @param credentialProvider credentials provider used to sign requests
     * @param clientConfig Client Configuration used when instantiating an AmazonKinesisClient
     * @param endpoint Amazon Kinesis endpoint to use
     * @param serviceName service name
     * @param regionId region id
     * @param describeStreamBackoffTimeInMillis backoff time for describing stream in millis
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
     */
    KinesisProxyFactory(AWSCredentialsProvider credentialProvider,
            ClientConfiguration clientConfig,
            String endpoint,
            String serviceName,
            String regionId,
            long describeStreamBackoffTimeInMillis,
            int maxDescribeStreamRetryAttempts) {
        this(credentialProvider, new AmazonKinesisClient(credentialProvider, clientConfig),
                describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts);
        this.kinesisClient.setEndpoint(endpoint, serviceName, regionId);
    }

    /**
     * Used internally in the class (and for development/testing).
     * 
     * @param credentialProvider credentials provider used to sign requests
     * @param client AmazonKinesisClient used to fetch data from Kinesis
     * @param describeStreamBackoffTimeInMillis backoff time for describing stream in millis
     * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
     */
    KinesisProxyFactory(AWSCredentialsProvider credentialProvider,
            AmazonKinesisClient client,
            long describeStreamBackoffTimeInMillis,
            int maxDescribeStreamRetryAttempts) {
        super();
        this.kinesisClient = client;
        this.credentialProvider = credentialProvider;
        this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
        this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IKinesisProxy getProxy(String streamName) {
        return new KinesisProxy(streamName,
                credentialProvider,
                kinesisClient,
                describeStreamBackoffTimeInMillis,
                maxDescribeStreamRetryAttempts);

    }
}
