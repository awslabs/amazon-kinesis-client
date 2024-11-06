/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.multilang.config.converter;

import java.time.Duration;

import org.apache.commons.beanutils.Converter;

/**
 * Converter that converts Duration text representation to a Duration object.
 * Refer to {@code Duration.parse} javadocs for the exact text representation.
 */
public class DurationConverter implements Converter {

    @Override
    public <T> T convert(Class<T> type, Object value) {
        if (value == null) {
            return null;
        }

        if (type != Duration.class) {
            throw new ConversionException("Can only convert to Duration");
        }

        String durationString = value.toString().trim();
        final Duration duration = Duration.parse(durationString);
        if (duration.isNegative()) {
            throw new ConversionException("Negative values are not permitted for duration: " + durationString);
        }

        return type.cast(duration);
    }

    public static class ConversionException extends RuntimeException {
        public ConversionException(String message) {
            super(message);
        }
    }
}
