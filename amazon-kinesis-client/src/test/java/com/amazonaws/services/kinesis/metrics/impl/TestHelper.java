/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.metrics.impl;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;

public class TestHelper {
    public static MetricDatum constructDatum(String name,
            StandardUnit unit,
            double maximum,
            double minimum,
            double sum,
            double count) {
        return new MetricDatum().withMetricName(name)
                .withUnit(unit)
                .withStatisticValues(new StatisticSet().withMaximum(maximum)
                        .withMinimum(minimum)
                        .withSum(sum)
                        .withSampleCount(count));
    }

    public static Dimension constructDimension(String name, String value) {
        return new Dimension().withName(name).withValue(value);
    }
}
