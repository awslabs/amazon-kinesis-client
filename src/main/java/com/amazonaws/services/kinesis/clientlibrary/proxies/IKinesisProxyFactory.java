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

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/** 
 * Interface for a KinesisProxyFactory.
 *
 */
public interface IKinesisProxyFactory {

    /**
     * Return an IKinesisProxy object for the specified stream.
     * @param streamName Stream from which data is consumed.
     * @return IKinesisProxy object.
     */
    @Deprecated
    IKinesisProxy getProxy(String streamName);

    /**
     * Return an IKinesisProxy object from the config object.
     * @return
     */
    IKinesisProxy getProxy();

}
