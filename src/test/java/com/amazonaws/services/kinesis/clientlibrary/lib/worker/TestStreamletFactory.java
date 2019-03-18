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
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Factory for TestStreamlet record processors.
 */
class TestStreamletFactory implements IRecordProcessorFactory {

    // Will be passed to the TestStreamlet. Can be used to check if all records have been processed.
    private Semaphore semaphore;
    private ShardSequenceVerifier shardSequenceVerifier;
    List<TestStreamlet> testStreamlets = new ArrayList<>();
    
    /**
     *  Constructor.
     */
    TestStreamletFactory(Semaphore semaphore, ShardSequenceVerifier shardSequenceVerifier) {
        this.semaphore = semaphore;
        this.shardSequenceVerifier = shardSequenceVerifier;
    }

    @Override
    public synchronized IRecordProcessor createProcessor() {
        TestStreamlet processor = new TestStreamlet(semaphore, shardSequenceVerifier);
        testStreamlets.add(processor);
        return processor;
    }

    Semaphore getSemaphore() {
        return semaphore;
    }

    ShardSequenceVerifier getShardSequenceVerifier() {
        return shardSequenceVerifier;
    }

    /**
     * @return the testStreamlets
     */
    List<TestStreamlet> getTestStreamlets() {
        return testStreamlets;
    }

}
