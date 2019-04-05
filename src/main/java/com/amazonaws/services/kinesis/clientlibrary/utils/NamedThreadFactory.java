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
