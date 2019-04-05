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

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

/**
 * Metrics scope that delegates to another metrics scope and is thread safe to be shared
 * across different threads.
 */
public class ThreadSafeMetricsDelegatingScope implements IMetricsScope {

    /** Metrics scope to delegate to. */
    private final IMetricsScope delegate;

    /**
     * Creates an instance of the metrics scope.
     * @param delegate metrics scope to delegate to
     */
    public ThreadSafeMetricsDelegatingScope(IMetricsScope delegate) {
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
