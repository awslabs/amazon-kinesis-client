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
package com.amazonaws.services.kinesis.metrics.impl;

/**
 * This is a MetricScope with a KeyType of String. It provides the implementation of
 * getting the key based off of the String KeyType.
 */

public abstract class AccumulateByNameMetricsScope extends AccumulatingMetricsScope<String> {

    @Override
    protected String getKey(String name) {
        return name;
    }

}
