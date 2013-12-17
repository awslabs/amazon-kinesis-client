/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;

/**
 * MetricsHelper assists with common metrics operations, most notably the storage of IMetricsScopes objects in a
 * ThreadLocal so we don't have to pass one throughout the whole call stack.
 */
public class MetricsHelper {

    private static final Log LOG = LogFactory.getLog(MetricsHelper.class);
    private static final NullMetricsScope NULL_METRICS_SCOPE = new NullMetricsScope();

    private static final ThreadLocal<IMetricsScope> currentScope = new ThreadLocal<IMetricsScope>();
    private static final ThreadLocal<Integer> referenceCount = new ThreadLocal<Integer>();

    /*
     * Constants used to publish metrics.
     */
    public static final String OPERATION_DIMENSION_NAME = "Operation";
    public static final String TIME = "Time";
    public static final String SUCCESS = "Success";
    private static final String SEP = ".";

    public static IMetricsScope startScope(IMetricsFactory factory) {
        return startScope(factory, null);
    }

    public static IMetricsScope startScope(IMetricsFactory factory, String operation) {
        IMetricsScope result = currentScope.get();
        if (result == null) {
            result = factory.createMetrics();
            if (operation != null) {
                result.addDimension(OPERATION_DIMENSION_NAME, operation);
            }
            currentScope.set(result);
            referenceCount.set(1);
        } else {
            referenceCount.set(referenceCount.get() + 1);
        }

        return result;
    }

    public static IMetricsScope getMetricsScope() {
        IMetricsScope result = currentScope.get();
        if (result == null) {
            LOG.warn(String.format("No metrics scope set in thread %s, getMetricsScope returning NullMetricsScope.",
                    Thread.currentThread().getName()));

            return NULL_METRICS_SCOPE;
        } else {
            return result;
        }
    }

    public static void addSuccessAndLatency(long startTimeMillis, boolean success) {
        addSuccessAndLatency(null, startTimeMillis, success);
    }

    public static void addSuccessAndLatency(String prefix, long startTimeMillis, boolean success) {
        addSuccessAndLatencyPerShard(null, prefix, startTimeMillis, success);
    }
    
    public static void addSuccessAndLatencyPerShard (
            String shardId, 
            String prefix, 
            long startTimeMillis, 
            boolean success) {
        IMetricsScope scope = getMetricsScope();

        String realPrefix = prefix == null ? "" : prefix + SEP;
        
        if (shardId != null) {
            scope.addDimension("ShardId", shardId);
        }

        scope.addData(realPrefix + MetricsHelper.SUCCESS, success ? 1 : 0, StandardUnit.Count);
        scope.addData(realPrefix + MetricsHelper.TIME,
                System.currentTimeMillis() - startTimeMillis,
                StandardUnit.Milliseconds);
    }

    public static void endScope() {
        IMetricsScope scope = getMetricsScope();
        if (scope != null) {
            Integer refCount = referenceCount.get();
            refCount--;

            if (refCount == 0) {
                scope.end();
                currentScope.remove();
            }
        }
    }

}
