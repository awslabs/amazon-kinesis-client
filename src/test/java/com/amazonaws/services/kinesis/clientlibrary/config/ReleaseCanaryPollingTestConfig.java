package com.amazonaws.services.kinesis.clientlibrary.config;

import software.amazon.awssdk.http.Protocol;

import java.util.UUID;

public class ReleaseCanaryPollingTestConfig extends KCLAppConfig {

    private final UUID uniqueId = UUID.randomUUID();
    @Override
    public String getStreamName() {
        return "KCLReleaseCanary1XPollingTestStream_" + uniqueId;
    }

}
