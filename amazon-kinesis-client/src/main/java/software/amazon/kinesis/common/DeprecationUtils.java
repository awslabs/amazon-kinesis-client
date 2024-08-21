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

import software.amazon.awssdk.utils.Either;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;

/**
 * Utility methods to facilitate deprecated code until that deprecated code
 * can be safely removed.
 */
public final class DeprecationUtils {

    private DeprecationUtils() {
        throw new UnsupportedOperationException("utility class");
    }

    /**
     * Converts a {@link StreamTracker} into the deprecated {@code Either<L, R>} convention.
     *
     * @param streamTracker tracker to convert
     */
    @Deprecated
    public static <R> Either<MultiStreamTracker, R> convert(
            StreamTracker streamTracker, Function<SingleStreamTracker, R> converter) {
        if (streamTracker instanceof MultiStreamTracker) {
            return Either.left((MultiStreamTracker) streamTracker);
        } else if (streamTracker instanceof SingleStreamTracker) {
            return Either.right(converter.apply((SingleStreamTracker) streamTracker));
        } else {
            throw new IllegalArgumentException("Unhandled StreamTracker: " + streamTracker);
        }
    }
}
