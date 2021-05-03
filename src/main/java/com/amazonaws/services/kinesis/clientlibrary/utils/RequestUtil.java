package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.AmazonWebServiceResult;

/**
 * Helper class to parse metadata from AWS requests.
 */
public class RequestUtil {
    private static final String DEFAULT_REQUEST_ID = "NONE";

    /**
     * Get the requestId associated with a request.
     *
     * @param result
     * @return the requestId for a request, or "NONE" if one is not available.
     */
    public static String requestId(AmazonWebServiceResult result) {
        if (result == null || result.getSdkResponseMetadata() == null || result.getSdkResponseMetadata().getRequestId() == null) {
            return DEFAULT_REQUEST_ID;
        }

        return result.getSdkResponseMetadata().getRequestId();
    }
}
