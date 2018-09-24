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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.retrieval.AWSExceptionManager;

/**
 *
 */
@RequiredArgsConstructor
@Slf4j
@Accessors(fluent = true)
@KinesisClientInternalApi
public class KinesisShardDetector implements ShardDetector {
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    @NonNull
    private final String streamName;
    private final long listShardsBackoffTimeInMillis;
    private final int maxListShardsRetryAttempts;
    private final long listShardsCacheAllowedAgeInSeconds;
    private final int maxCacheMissesBeforeReload;
    private final int cacheMissWarningModulus;

    private volatile Map<String, Shard> cachedShardMap = null;
    private volatile Instant lastCacheUpdateTime;
    @Getter(AccessLevel.PACKAGE)
    private AtomicInteger cacheMisses = new AtomicInteger(0);

    @Override
    public Shard shard(@NonNull final String shardId) {
        if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
            synchronized (this) {
                if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
                    listShards();
                }
            }
        }

        Shard shard = cachedShardMap.get(shardId);

        if (shard == null) {
            if (cacheMisses.incrementAndGet() > maxCacheMissesBeforeReload || shouldRefreshCache()) {
                synchronized (this) {
                    shard = cachedShardMap.get(shardId);

                    if (shard == null) {
                        log.info("Too many shard map cache misses or cache is out of date -- forcing a refresh");
                        listShards();
                        shard = cachedShardMap.get(shardId);

                        if (shard == null) {
                            log.warn("Even after cache refresh shard '{}' wasn't found. This could indicate a bigger"
                                    + " problem.", shardId);
                        }

                        cacheMisses.set(0);
                    } else {
                        //
                        // If the shardmap got updated, go ahead and set cache misses to 0
                        //
                        cacheMisses.set(0);
                    }
                }
            }
        }

        if (shard == null) {
            final String message = String.format("Cannot find the shard given the shardId %s. Cache misses: %s",
                    shardId, cacheMisses);
            if (cacheMisses.get() % cacheMissWarningModulus == 0) {
                log.warn(message);
            } else {
                log.debug(message);
            }
        }

        return shard;
    }

    @Override
    @Synchronized
    public List<Shard> listShards() {
        final List<Shard> shards = new ArrayList<>();
        ListShardsResponse result;
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
                shards.addAll(result.shards());
                nextToken = result.nextToken();
            }
        } while (StringUtils.isNotEmpty(result.nextToken()));

        cachedShardMap(shards);
        return shards;
    }

    private ListShardsResponse listShards(final String nextToken) {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(LimitExceededException.class, t -> t);
        exceptionManager.add(ResourceInUseException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);

        ListShardsRequest.Builder request = KinesisRequestsBuilder.listShardsRequestBuilder();
        if (StringUtils.isEmpty(nextToken)) {
            request = request.streamName(streamName);
        } else {
            request = request.nextToken(nextToken);
        }
        ListShardsResponse result = null;
        LimitExceededException lastException = null;
        int remainingRetries = maxListShardsRetryAttempts;

        while (result == null) {

            try {
                try {
                    result = kinesisClient.listShards(request.build()).get();
                } catch (ExecutionException e) {
                    throw exceptionManager.apply(e.getCause());
                } catch (InterruptedException e) {
                    // TODO: check if this is the correct behavior for Interrupted Exception
                    log.debug("Interrupted exception caught, shutdown initiated, returning null");
                    return null;
                }
            } catch (ResourceInUseException e) {
                log.info("Stream is not in Active/Updating status, returning null (wait until stream is in"
                        + " Active or Updating)");
                return null;
            } catch (LimitExceededException e) {
                log.info("Got LimitExceededException when listing shards {}. Backing off for {} millis.", streamName,
                        listShardsBackoffTimeInMillis);
                try {
                    Thread.sleep(listShardsBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    log.debug("Stream {} : Sleep  was interrupted ", streamName, ie);
                }
                lastException = e;
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

    void cachedShardMap(final List<Shard> shards) {
        cachedShardMap = shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
        lastCacheUpdateTime = Instant.now();
    }

    private boolean shouldRefreshCache() {
        final Duration secondsSinceLastUpdate = Duration.between(lastCacheUpdateTime, Instant.now());
        final String message = String.format("Shard map cache is %d seconds old", secondsSinceLastUpdate.getSeconds());
        if (secondsSinceLastUpdate.compareTo(Duration.of(listShardsCacheAllowedAgeInSeconds, ChronoUnit.SECONDS)) > 0) {
            log.info("{}. Age exceeds limit of {} seconds -- Refreshing.", message, listShardsCacheAllowedAgeInSeconds);
            return true;
        }

        log.debug("{}. Age doesn't exceed limit of {} seconds.", message, listShardsCacheAllowedAgeInSeconds);
        return false;
    }
}
