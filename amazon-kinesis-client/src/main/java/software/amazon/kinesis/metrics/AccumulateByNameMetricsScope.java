/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.metrics;

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
