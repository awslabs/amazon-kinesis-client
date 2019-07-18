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

import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.LeasesCacheException;
import com.amazonaws.services.kinesis.leases.impl.Lease;

/**
 * Cache interface for caching leases
 *
 * @param <T>
 */
public interface ILeasesCache<T extends Lease> {

    /**
     * @param key Key to look for in the cache
     * @return Returns the List of leases mapped to the key. If not found in the
     *         cache, loads from the source registered with the cache
     * @throws LeasesCacheException
     */
    List<T> getLeases(String key) throws LeasesCacheException;
}
