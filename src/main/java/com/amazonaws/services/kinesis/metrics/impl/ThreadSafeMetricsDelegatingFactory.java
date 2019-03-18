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

import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;

/**
 * Metrics scope factory that delegates metrics scope creation to another factory, but
 * returns metrics scope that is thread safe.
 */
public class ThreadSafeMetricsDelegatingFactory implements IMetricsFactory {

    /** Metrics factory to delegate to. */
    private final IMetricsFactory delegate;

    /**
     * Creates an instance of the metrics factory.
     * @param delegate metrics factory to delegate to
     */
    public ThreadSafeMetricsDelegatingFactory(IMetricsFactory delegate) {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMetricsScope createMetrics() {
        return new ThreadSafeMetricsDelegatingScope(delegate.createMetrics());
    }
}
