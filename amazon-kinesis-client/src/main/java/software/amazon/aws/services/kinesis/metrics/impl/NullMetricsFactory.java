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
package software.amazon.aws.services.kinesis.metrics.impl;

import software.amazon.aws.services.kinesis.metrics.interfaces.IMetricsFactory;
import software.amazon.aws.services.kinesis.metrics.interfaces.IMetricsScope;

public class NullMetricsFactory implements IMetricsFactory {

    private static final NullMetricsScope SCOPE = new NullMetricsScope();

    @Override
    public IMetricsScope createMetrics() {
        return SCOPE;
    }

}
