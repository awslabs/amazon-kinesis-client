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
package software.amazon.kinesis.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * Transforms KCL metric names into OTel-compliant instrument names.
 *
 * <p>The transformation pipeline (in order):
 * <ol>
 *   <li>Special-case rename map — if hit, skip steps 2-5 and go to step 6.</li>
 *   <li>Substitute {@code :} with {@code .}</li>
 *   <li>Replace trailing {@code .Time} with {@code .duration}</li>
 *   <li>Strip trailing {@code .Count}</li>
 *   <li>Split PascalCase/camelCase — insert {@code _} at lowercase→uppercase transitions
 *       within each dot-separated segment, treating runs of consecutive uppercase as one word.</li>
 *   <li>Lowercase the entire string.</li>
 *   <li>Prepend namespace prefix {@code aws.kinesis.client.}</li>
 *   <li>Validate against OTel BNF and 255 char limit.</li>
 * </ol>
 */
@Slf4j
class OtelMetricNameTransformer {

    private static final String NAMESPACE_PREFIX = "aws.kinesis.client.";

    /**
     * OTel instrument name BNF: [A-Za-z][A-Za-z0-9_.\-/]{0,254}
     */
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9_.\\-/]{0,254}");

    /**
     * Pattern matching characters that are illegal in an OTel instrument name.
     */
    private static final Pattern ILLEGAL_CHARS_PATTERN = Pattern.compile("[^A-Za-z0-9_.\\-/]");

    private static final Map<String, String> SPECIAL_CASE_MAP;

    static {
        Map<String, String> map = new HashMap<>();
        map.put("MillisBehindLatest", "consumer.lag.duration");
        map.put("DataBytesProcessed", "records.size");
        map.put("ActiveStreams.Count", "streams.active");
        map.put("StreamsPendingDeletion.Count", "streams.pending_deletion");
        map.put("NonExistingStreamDelete.Count", "streams.deleted_nonexistent");
        map.put("DeletedStreams.Count", "streams.deleted");
        map.put("NumStreamsToSync", "streams.pending_sync");
        map.put("QueueSize", "stream_id_cache.queue.size");
        map.put("NumWorkers", "workers");
        map.put("NumWorkersWithInvalidEntry", "workers.invalid_entry");
        map.put("NumWorkersWithFailingWorkerMetric", "workers.failing_worker_metric");
        SPECIAL_CASE_MAP = Collections.unmodifiableMap(map);
    }

    /**
     * Transforms a KCL metric name to an OTel-compliant instrument name.
     *
     * @param kclName the original KCL metric name
     * @return the transformed OTel instrument name
     */
    static String transformName(String kclName) {
        String name;

        // Step 1: Special-case rename map
        String specialCase = SPECIAL_CASE_MAP.get(kclName);
        if (specialCase != null) {
            // Skip steps 2-5, go straight to step 6 (lowercase) and step 7 (prefix)
            name = specialCase;
        } else {
            name = kclName;

            // Step 2: Substitute ':' with '.'
            name = name.replace(':', '.');

            // Step 3: Replace trailing '.Time' with '.duration'
            if (name.endsWith(".Time")) {
                name = name.substring(0, name.length() - ".Time".length()) + ".duration";
            }

            // Step 4: Strip trailing '.Count'
            if (name.endsWith(".Count")) {
                name = name.substring(0, name.length() - ".Count".length());
            }

            // Step 5: Split PascalCase/camelCase within each dot-separated segment
            name = splitCamelCase(name);

            // Step 6: Lowercase
            name = name.toLowerCase();
        }

        // Step 7: Prepend namespace prefix
        name = NAMESPACE_PREFIX + name;

        // Step 8: Validate
        name = validate(name);

        return name;
    }

    /**
     * Transforms a KCL metric name to an OTel-compliant instrument name.
     * Currently delegates to {@link #transformName(String)}; the unit parameter is reserved
     * for future use.
     *
     * @param kclName the original KCL metric name
     * @param unit    the StandardUnit (reserved for future use)
     * @return the transformed OTel instrument name
     */
    static String transformName(String kclName, StandardUnit unit) {
        return transformName(kclName);
    }

    /**
     * Splits PascalCase/camelCase within each dot-separated segment by inserting underscores.
     * Treats runs of consecutive uppercase letters as a single word
     * (e.g., "GSIReadyStatus" → "GSI_Ready_Status", not "G_S_I_Ready_Status").
     */
    private static String splitCamelCase(String input) {
        String[] segments = input.split("\\.", -1);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                result.append('.');
            }
            result.append(splitSegment(segments[i]));
        }
        return result.toString();
    }

    /**
     * Splits a single segment (no dots) at camelCase boundaries.
     * Rules:
     * - Insert '_' before a transition from lowercase to uppercase.
     * - Insert '_' before the last char of an uppercase run followed by a lowercase char
     *   (e.g., "GSIReady" → "GSI_Ready").
     */
    private static String splitSegment(String segment) {
        if (segment.isEmpty()) {
            return segment;
        }
        StringBuilder sb = new StringBuilder();
        char[] chars = segment.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (i > 0 && Character.isUpperCase(chars[i])) {
                // Insert '_' at lowercase→uppercase transition
                if (Character.isLowerCase(chars[i - 1])) {
                    sb.append('_');
                }
                // Insert '_' before the last uppercase in a run followed by lowercase
                // e.g., "GSIReady": at 'R' (i=3), chars[2]='I' is upper, chars[3]='R' is upper,
                // but we need to check if chars[i] starts a new word.
                // Actually: at 'R', prev is 'I' (upper), and next is 'e' (lower) → insert '_'
                else if (Character.isUpperCase(chars[i - 1])
                        && i + 1 < chars.length
                        && Character.isLowerCase(chars[i + 1])) {
                    sb.append('_');
                }
            }
            sb.append(chars[i]);
        }
        return sb.toString();
    }

    /**
     * Validates the name against OTel BNF and 255 char limit.
     * If invalid, replaces illegal characters with '_' and logs a warning.
     */
    private static String validate(String name) {
        if (name.length() > 255) {
            log.warn("OTel metric name exceeds 255 characters, truncating: {}", name);
            name = name.substring(0, 255);
        }
        if (!VALID_NAME_PATTERN.matcher(name).matches()) {
            String original = name;
            name = ILLEGAL_CHARS_PATTERN.matcher(name).replaceAll("_");
            // Ensure it starts with a letter
            if (name.isEmpty() || !Character.isLetter(name.charAt(0))) {
                name = "m" + name;
            }
            if (name.length() > 255) {
                name = name.substring(0, 255);
            }
            log.warn("OTel metric name contained illegal characters, sanitized '{}' to '{}'", original, name);
        }
        return name;
    }
}
