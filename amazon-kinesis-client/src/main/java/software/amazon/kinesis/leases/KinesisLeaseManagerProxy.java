/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.kinesis.leases;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.Shard;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@RequiredArgsConstructor
@Slf4j
public class KinesisLeaseManagerProxy implements LeaseManagerProxy {
    @NonNull
    private final AmazonKinesis amazonKinesis;
    @NonNull
    final String streamName;
    final long listShardsBackoffTimeInMillis;
    final int maxListShardsRetryAttempts;

    @Override
    public List<Shard> listShards() {
        final List<Shard> shards = new ArrayList<>();
        ListShardsResult result;
        String nextToken = null;

        do {
            result = listShards(nextToken);

            if (result == null) {
                    /*
                    * If listShards ever returns null, we should bail and return null. This indicates the stream is not
                    * in ACTIVE or UPDATING state and we may not have accurate/consistent information about the stream.
                    */
                return null;
            } else {
                shards.addAll(result.getShards());
                nextToken = result.getNextToken();
            }
        } while (StringUtils.isNotEmpty(result.getNextToken()));
        return shards;
    }

    private ListShardsResult listShards(final String nextToken) {
        final ListShardsRequest request = new ListShardsRequest();
        if (StringUtils.isEmpty(nextToken)) {
            request.setStreamName(streamName);
        } else {
            request.setNextToken(nextToken);
        }
        ListShardsResult result = null;
        LimitExceededException lastException = null;
        int remainingRetries = maxListShardsRetryAttempts;

        while (result == null) {
            try {
                result = amazonKinesis.listShards(request);
            } catch (LimitExceededException e) {
                log.info("Got LimitExceededException when listing shards {}. Backing off for {} millis.", streamName,
                        listShardsBackoffTimeInMillis);
                try {
                    Thread.sleep(listShardsBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    log.debug("Stream {} : Sleep  was interrupted ", streamName, ie);
                }
                lastException = e;
            } catch (ResourceInUseException e) {
                log.info("Stream is not in Active/Updating status, returning null (wait until stream is in Active or"
                        + " Updating)");
                return null;
            }
            remainingRetries--;
            if (remainingRetries <= 0 && result == null) {
                if (lastException != null) {
                    throw lastException;
                }
                throw new IllegalStateException("Received null from ListShards call.");
            }
        }

        return result;
    }
}
