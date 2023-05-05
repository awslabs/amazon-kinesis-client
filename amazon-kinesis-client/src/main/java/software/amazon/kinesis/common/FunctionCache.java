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

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

/**
 * Caches the result from a {@link Function}. Caching is especially useful when
 * invoking the function is an expensive call that produces a reusable result.
 * If the input value should be fixed, {@link SupplierCache} may be used.
 * <br/><br/>
 * Note that if {@code f(x)=X} is cached, {@code X} will be returned for every
 * successive query of this cache regardless of the input parameter. This is
 * by design under the assumption that {@code X} is a viable response for
 * other invocations.
 *
 * @param <IN> input type
 * @param <OUT> output type
 */
@RequiredArgsConstructor
public class FunctionCache<IN, OUT> extends SynchronizedCache<OUT> {

    private final Function<IN, OUT> function;

    /**
     * Returns the cached result. If the cache is null, the function will be
     * invoked to populate the cache.
     *
     * @param input input argument to the underlying function
     * @return cached result which may be null
     */
    public OUT get(final IN input) {
        return get(() -> function.apply(input));
    }

}
