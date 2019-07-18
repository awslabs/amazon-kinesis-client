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
package com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync;

import java.util.concurrent.Callable;

/**
 * Interface to schedule tasks e.g ShardSync
 *
 * @param <T>
 */
public interface TaskSchedulerStrategy<T extends Callable> {
	
    /**
     * @param task Task to be scheduled
     */
	void scheduleTask(T task);

    /**
     * Start the scheduling of tasks
     */
    void start();

    /**
     * Stop the scheduled tasks
     */
    void stop();

    /*
    Cleans up any open resources
     */
    void shutdown();
}
