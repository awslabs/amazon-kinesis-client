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


import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * Metrics scope that delegates to another metrics scope and is thread safe to be shared
 * across different threads.
 */
public class ThreadSafeMetricsDelegatingScope implements MetricsScope {

    /** Metrics scope to delegate to. */
    private final MetricsScope delegate;

    /**
     * Creates an instance of the metrics scope.
     * @param delegate metrics scope to delegate to
     */
    public ThreadSafeMetricsDelegatingScope(MetricsScope delegate) {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void addData(String name, double value, StandardUnit unit) {
        delegate.addData(name, value, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
        delegate.addData(name, value, unit, level);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void addDimension(String name, String value) {
        delegate.addDimension(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void end() {
        delegate.end();
    }

}
