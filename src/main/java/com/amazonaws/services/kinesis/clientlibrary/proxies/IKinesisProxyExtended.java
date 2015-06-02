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

import com.amazonaws.services.kinesis.model.Shard;

/**
 * Kinesis proxy interface extended with addition method(s). Operates on a
 * single stream (set up at initialization).
 * 
 */
public interface IKinesisProxyExtended extends IKinesisProxy {

    /**
     * Get the Shard corresponding to shardId associated with this
     * IKinesisProxy.
     * 
     * @param shardId
     *            Fetch the Shard with this given shardId
     * @return the Shard with the given shardId
     */
    Shard getShard(String shardId);
}
