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


import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

public class TestHelper {
    public static MetricDatum constructDatum(String name,
                                             StandardUnit unit,
                                             double maximum,
                                             double minimum,
                                             double sum,
                                             double count) {
        return MetricDatum.builder().metricName(name)
                .unit(unit)
                .statisticValues(StatisticSet.builder().maximum(maximum)
                        .minimum(minimum)
                        .sum(sum)
                        .sampleCount(count).build()).build();
    }

    public static Dimension constructDimension(String name, String value) {
        return Dimension.builder().name(name).value(value).build();
    }
}
