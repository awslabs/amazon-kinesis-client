/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

public abstract class InterceptingMetricsFactory implements IMetricsFactory {

    private final IMetricsFactory other;

    public InterceptingMetricsFactory(IMetricsFactory other) {
        this.other = other;
    }

    @Override
    public IMetricsScope createMetrics() {
        IMetricsScope otherScope = other.createMetrics();
        interceptCreateMetrics(otherScope);
        return new InterceptingMetricsScope(otherScope);
    }

    protected void interceptCreateMetrics(IMetricsScope scope) {
        // Default implementation does nothing;
    }

    protected void interceptAddData(String name, double value, StandardUnit unit, IMetricsScope scope) {
        scope.addData(name, value, unit);
    }

    protected void interceptAddData(String name, double value, StandardUnit unit, MetricsLevel level, IMetricsScope scope) {
        scope.addData(name, value, unit, level);
    }

    protected void interceptAddDimension(String name, String value, IMetricsScope scope) {
        scope.addDimension(name, value);
    }

    protected void interceptEnd(IMetricsScope scope) {
        scope.end();
    }

    private class InterceptingMetricsScope implements IMetricsScope {

        private IMetricsScope other;

        public InterceptingMetricsScope(IMetricsScope other) {
            this.other = other;
        }

        @Override
        public void addData(String name, double value, StandardUnit unit) {
            interceptAddData(name, value, unit, other);
        }

        @Override
        public void addData(String name, double value, StandardUnit unit, MetricsLevel level) {
            interceptAddData(name, value, unit, level, other);
        }

        @Override
        public void addDimension(String name, String value) {
            interceptAddDimension(name, value, other);
        }

        @Override
        public void end() {
            interceptEnd(other);
        }

    }

}
