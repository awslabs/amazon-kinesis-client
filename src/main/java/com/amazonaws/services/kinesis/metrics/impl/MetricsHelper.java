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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

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
    public static final String SHARD_ID_DIMENSION_NAME = "ShardId";
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

    /**
     * Sets given metrics scope for the current thread.
     *
     * Method must be used with care. Metrics helper is designed such that separate metrics scopes are associated
     * with each thread. However, when sharing metrics scope and setting it explicitly on a thread, thread safety must
     * also be taken into account.
     * @param scope
     */
    public static void setMetricsScope(IMetricsScope scope) {
        if (isMetricsScopePresent()) {
            throw new RuntimeException(String.format(
                    "Metrics scope is already set for the current thread %s", Thread.currentThread().getName()));
        }
        currentScope.set(scope);
    }

    /**
     * Checks if current metricsscope is present or not.
     * 
     * @return true if metrics scope is present, else returns false
     */
    public static boolean isMetricsScopePresent() {
        return currentScope.get() != null;
    }

    /**
     * Unsets the metrics scope for the current thread.
     */
    public static void unsetMetricsScope() {
        currentScope.remove();
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

    public static void addSuccessAndLatency(long startTimeMillis, boolean success, MetricsLevel level) {
        addSuccessAndLatency(null, startTimeMillis, success, level);
    }

    public static void addSuccessAndLatency(
            String prefix, long startTimeMillis, boolean success, MetricsLevel level) {
        addSuccessAndLatencyPerShard(null, prefix, startTimeMillis, success, level);
    }

    public static void addSuccessAndLatencyPerShard (
            String shardId,
            String prefix,
            long startTimeMillis,
            boolean success,
            MetricsLevel level) {
        addSuccessAndLatency(shardId, prefix, startTimeMillis, success, level, true, true);
    }

    public static void addLatency(long startTimeMillis, MetricsLevel level) {
        addLatency(null, startTimeMillis, level);
    }

    public static void addLatency(String prefix, long startTimeMillis, MetricsLevel level) {
        addLatencyPerShard(null, prefix, startTimeMillis, level);
    }

    public static void addLatencyPerShard(String shardId, String prefix, long startTimeMillis, MetricsLevel level) {
        addSuccessAndLatency(shardId, prefix, startTimeMillis, false, level, false, true);
    }

    private static void addSuccessAndLatency(
            String shardId, String prefix, long startTimeMillis, boolean success, MetricsLevel level,
            boolean includeSuccess, boolean includeLatency) {
        IMetricsScope scope = getMetricsScope();

        String realPrefix = prefix == null ? "" : prefix + SEP;

        if (shardId != null) {
            scope.addDimension(SHARD_ID_DIMENSION_NAME, shardId);
        }
        if (includeSuccess) {
            scope.addData(realPrefix + MetricsHelper.SUCCESS, success ? 1 : 0, StandardUnit.Count, level);
        }
        if (includeLatency) {
            scope.addData(realPrefix + MetricsHelper.TIME,
                    System.currentTimeMillis() - startTimeMillis, StandardUnit.Milliseconds, level);
        }
    }

    public static void endScope() {
        IMetricsScope scope = getMetricsScope();
        if (scope != null) {
            referenceCount.set(referenceCount.get() - 1);

            if (referenceCount.get() == 0) {
                scope.end();
                currentScope.remove();
            }
        }
    }

}
