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

/**
 * This class defines a set of standard metrics levels that can be used to control which metrics get emitted.
 * {@code MetricsLevel} objects are ordered and are specified by ordered integers. Enabling metrics at a given
 * level also enables the metrics at all higher levels.
 * <p>
 * Metrics levels in descending order are:
 * <ul>
 * <li>SUMMARY</li>
 * <li>DETAILED</li>
 * </ul>
 * In addition, NONE level can be used to turn off all metrics.
 */
public enum MetricsLevel {

    /**
     * NONE metrics level can be used to turn off metrics.
     */
    NONE("NONE", Integer.MAX_VALUE),

    /**
     * SUMMARY metrics level can be used to emit only the most significant metrics.
     */
    SUMMARY("SUMMARY", 10000),

    /**
     * DETAILED metrics level can be used to emit all metrics.
     */
    DETAILED("DETAILED", 9000);

    /**
     * Name of the metrics level.
     */
    private final String name;

    /**
     * Integer value of the metrics level.
     */
    private final int value;

    /**
     * Creates metrics level with given name and value.
     * @param name Metrics level name
     * @param value Metrics level value
     */
    private MetricsLevel(String name, int value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns the name for this metrics level.
     * @return Returns the name for this metrics level.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the value for this metrics level.
     * @return Returns the value for this metrics level.
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns metrics level associated with the given name.
     * @param name Name of the metrics level.
     * @return Returns metrics level associated with the given name.
     */
    public static MetricsLevel fromName(String name) {
        if (name != null) {
            name = name.toUpperCase();
        }
        return valueOf(name);
    }
}
