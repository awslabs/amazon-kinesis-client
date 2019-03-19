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

import software.amazon.awssdk.services.cloudwatch.model.Dimension;

import java.util.HashSet;
import java.util.Set;


/**
 * DimensionTrackingMetricsScope is where we provide functionality for dimensions.
 * Dimensions allow the user to be able view their metrics based off of the parameters they specify.
 * 
 * The following examples show how to add dimensions if they would like to view their all metrics
 * pertaining to a particular stream or for a specific date.
 * 
 * myScope.addDimension("StreamName", "myStreamName");
 * myScope.addDimension("Date", "Dec012013");
 * 
 * 
 */

public abstract class DimensionTrackingMetricsScope implements MetricsScope {

    private Set<Dimension> dimensions = new HashSet<>();

    @Override
    public void addDimension(String name, String value) {
        dimensions.add(Dimension.builder().name(name).value(value).build());
    }

    /**
     * @return a set of dimensions for an IMetricsScope
     */

    protected Set<Dimension> getDimensions() {
        return dimensions;
    }

}
