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
