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
