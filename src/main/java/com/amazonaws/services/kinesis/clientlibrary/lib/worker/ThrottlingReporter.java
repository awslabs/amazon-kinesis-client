package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class ThrottlingReporter {

    private final int maxConsecutiveWarnThrottles;

    @Getter
    private int consecutiveThrottles = 0;

    void throttled() {
        consecutiveThrottles++;
    }

    void success() {
        consecutiveThrottles = 0;
    }

    boolean shouldReportError() {
        return consecutiveThrottles > maxConsecutiveWarnThrottles;
    }

}
