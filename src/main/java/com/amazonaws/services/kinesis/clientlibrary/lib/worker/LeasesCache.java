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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.ILeasesCache;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Guava cache implementation for caching the leases
 */
class LeasesCache implements ILeasesCache<KinesisClientLease> {

    private static final Log LOG = LogFactory.getLog(LeasesCache.class);
    private LoadingCache<String, List<KinesisClientLease>> cache;

    /**
     * @param leaseManager to interact with the leases table
     * @param ttl time after which the cached leases expire after being written
     *            for the first time to the cache
     * @param timeUnit unit for ttl
     */
    LeasesCache(ILeaseManager<KinesisClientLease> leaseManager, int ttl, TimeUnit timeUnit) {
        CacheLoader<String, List<KinesisClientLease>> cacheLoader = new CacheLoader<String, List<KinesisClientLease>>() {

            @Override
            public List<KinesisClientLease> load(String key)
                    throws DependencyException, InvalidStateException, ProvisionedThroughputException {
                return leaseManager.listLeases();
            }
        };
        cache = CacheBuilder.newBuilder()
                            .expireAfterWrite(ttl, timeUnit)
                            .build(cacheLoader);
    }

    /*
     * Fetches the leases from the cache. If not found, fetches from the leases
     * table and caches it for the provided TTl
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync.
     * interfaces.ILeasesCache#getLeases(java.lang.String)
     */
    @Override
    public List<KinesisClientLease> getLeases(String key) throws LeasesCacheException {
        try {
            return cache.get(key);
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw new LeasesCacheException("Exception occurred while trying to fetch all leases from the leases cache",
                                           e.getCause());
        }
    }

}
