package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;

@RequiredArgsConstructor
@CommonsLog
class ThrottlingReporter {

    private final int maxConsecutiveWarnThrottles;
    private final String shardId;

    private int consecutiveThrottles = 0;

    void throttled() {
        consecutiveThrottles++;
        String message = "Shard '" + shardId + "' has been throttled "
                + consecutiveThrottles + " consecutively";

        if (consecutiveThrottles > maxConsecutiveWarnThrottles) {
            log.error(message);
        } else {
            log.warn(message);
        }

    }

    void success() {
        consecutiveThrottles = 0;
    }

}
