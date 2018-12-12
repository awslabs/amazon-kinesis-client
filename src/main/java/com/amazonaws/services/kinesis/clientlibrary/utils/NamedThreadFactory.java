/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Custom thread factory that sets thread names based on the specified prefix.
 */
public class NamedThreadFactory implements ThreadFactory {

    private String threadPrefix;
    private ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * Construct a thread factory that uses the specified parameter as the thread prefix.
     *
     * @param threadPrefix the prefix with witch all created threads will be named
     */
    public NamedThreadFactory(String threadPrefix) {
        this.threadPrefix = threadPrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = defaultFactory.newThread(r);
        thread.setName(threadPrefix + counter.incrementAndGet());
        return thread;
    }
}
