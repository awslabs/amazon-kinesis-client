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

import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.Converter;
import software.amazon.awssdk.services.dynamodb.model.Tag;

/**
 * Converter that converts to a Collection of Tag object.
 * The text format accepted are as follows:
 * tagPropertyName = key1=value1,key2=value2,...
 */
@Slf4j
public class TagConverter implements Converter {

    @Override
    public <T> T convert(Class<T> type, Object value) {
        if (value == null) {
            return null;
        }

        if (!type.isAssignableFrom(TagCollection.class)) {
            throw new ConversionException("Can only convert to Collection<Tag>");
        }

        final TagCollection collection = new TagCollection();
        final String tagString = value.toString().trim();
        final String[] keyValuePairs = tagString.split(",");
        for (String keyValuePair : keyValuePairs) {
            final String[] tokens = keyValuePair.trim().split("=");
            if (tokens.length != 2) {
                log.warn("Invalid tag {}, ignoring it", keyValuePair);
                continue;
            }
            final Tag tag =
                    Tag.builder().key(tokens[0].trim()).value(tokens[1].trim()).build();
            log.info("Created tag {}", tag);
            collection.add(tag);
        }

        return type.cast(collection);
    }

    public static class ConversionException extends RuntimeException {
        public ConversionException(String message) {
            super(message);
        }
    }

    public static class TagCollection extends ArrayList<Tag> {}
}
