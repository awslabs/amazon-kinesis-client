/*
 * Copyright 2023 Amazon.com, Inc. or its affiliates.
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
package software.amazon.kinesis.common;

import java.util.function.Supplier;

/**
 * A synchronized, "no frills" cache that preserves the first non-null value
 * returned from a {@link Supplier}.
 *
 * @param <R> result type
 */
public class SynchronizedCache<R> {

    private volatile R result;

    /**
     * Returns the cached result. If the cache is null, the supplier will be
     * invoked to populate the cache.
     *
     * @param supplier supplier to invoke if the cache is null
     * @return cached result which may be null
     */
    protected R get(final Supplier<R> supplier) {
        if (result == null) {
            synchronized (this) {
                // double-check lock
                if (result == null) {
                    result = supplier.get();
                }
            }
        }
        return result;
    }

}
