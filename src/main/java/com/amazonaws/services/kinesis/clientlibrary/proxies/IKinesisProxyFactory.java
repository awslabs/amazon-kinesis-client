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

/** 
 * Interface for a KinesisProxyFactory.
 *
 * @deprecated Deprecating since KinesisProxy is just created once, there is no use of a factory. There is no
 * replacement for this class. This class will be removed in the next major/minor release.
 * 
 */
@Deprecated
public interface IKinesisProxyFactory {

    /**
     * Return an IKinesisProxy object for the specified stream.
     * @param streamName Stream from which data is consumed.
     * @return IKinesisProxy object.
     */
    IKinesisProxy getProxy(String streamName);

}
