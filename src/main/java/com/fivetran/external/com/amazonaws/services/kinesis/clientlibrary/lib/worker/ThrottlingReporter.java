/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;

class ThrottlingReporter {

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SimpleLog.class);
    private final int maxConsecutiveWarnThrottles;
    private final String shardId;

    private int consecutiveThrottles = 0;

    public ThrottlingReporter(int maxConsecutiveWarnThrottles, String shardId) {
        this.maxConsecutiveWarnThrottles = maxConsecutiveWarnThrottles;
        this.shardId = shardId;
    }

    void throttled() {
        consecutiveThrottles++;
        String message = "Shard '" + shardId + "' has been throttled "
                + consecutiveThrottles + " consecutively";

        if (consecutiveThrottles > maxConsecutiveWarnThrottles) {
            getLog().error(message);
        } else {
            getLog().warn(message);
        }

    }

    void success() {
        consecutiveThrottles = 0;
    }

    protected Log getLog() {
        return log;
    }

}
