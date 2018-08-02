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

import java.util.List;
import java.util.Objects;

import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;



/*
 * A representation of a key of a MetricDatum. This class is useful when wanting to compare 
 * whether 2 keys have the same MetricDatum. This feature will be used in MetricAccumulatingQueue
 * where we aggregate metrics across multiple MetricScopes.
 */
public class CloudWatchMetricKey {

    private List<Dimension> dimensions;
    private String metricName;
    
    /**
     * @param datum data point
     */

    public CloudWatchMetricKey(MetricDatum datum) {
        this.dimensions = datum.dimensions();
        this.metricName = datum.metricName();
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensions, metricName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CloudWatchMetricKey other = (CloudWatchMetricKey) obj;
        return Objects.equals(other.dimensions, dimensions) && Objects.equals(other.metricName, metricName);
    }

}
