/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.leases;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.StreamIdentifier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisShardDetectorTest {

    private static final String STREAM_NAME = "TestStream";
    private static final long LIST_SHARDS_BACKOFF_TIME_IN_MILLIS = 50L;
    private static final int MAX_LIST_SHARDS_RETRY_ATTEMPTS = 5;
    private static final long LIST_SHARDS_CACHE_ALLOWED_AGE_IN_SECONDS = 10;
    private static final int MAX_CACHE_MISSES_BEFORE_RELOAD = 10;
    private static final int CACHE_MISS_WARNING_MODULUS = 2;
    private static final Duration KINESIS_REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final String SHARD_ID = "shardId-%012d";

    private KinesisShardDetector shardDetector;

    @Rule
    public ExpectedException expectedExceptionRule = ExpectedException.none();

    @Mock
    private KinesisAsyncClient client;

    @Mock
    private CompletableFuture<ListShardsResponse> mockFuture;

    @Before
    public void setup() {
        shardDetector = new KinesisShardDetector(
                client,
                StreamIdentifier.singleStreamInstance(STREAM_NAME),
                LIST_SHARDS_BACKOFF_TIME_IN_MILLIS,
                MAX_LIST_SHARDS_RETRY_ATTEMPTS,
                LIST_SHARDS_CACHE_ALLOWED_AGE_IN_SECONDS,
                MAX_CACHE_MISSES_BEFORE_RELOAD,
                CACHE_MISS_WARNING_MODULUS,
                KINESIS_REQUEST_TIMEOUT);
    }

    @Test
    public void testListShardsSingleResponse() {
        final List<Shard> expectedShards = new ArrayList<>();
        final ListShardsResponse listShardsResponse = ListShardsResponse.builder()
                .nextToken(null)
                .shards(expectedShards)
                .build();
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.completedFuture(listShardsResponse);

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        final List<Shard> shards = shardDetector.listShards();

        assertThat(shards, equalTo(expectedShards));
        verify(client).listShards(any(ListShardsRequest.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testListShardsNullResponse() {
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.completedFuture(null);

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        try {
            shardDetector.listShards();
        } finally {
            verify(client, times(MAX_LIST_SHARDS_RETRY_ATTEMPTS)).listShards(any(ListShardsRequest.class));
        }
    }

    @Test
    public void testListShardsResouceInUse() {
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.supplyAsync(() -> {
            throw ResourceInUseException.builder().build();
        });

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        final List<Shard> shards = shardDetector.listShards();

        assertThat(shards, nullValue());
        verify(client).listShards(any(ListShardsRequest.class));
    }

    @Test(expected = LimitExceededException.class)
    public void testListShardsThrottled() {
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.supplyAsync(() -> {
            throw LimitExceededException.builder().build();
        });

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        try {
            shardDetector.listShards();
        } finally {
            verify(client, times(MAX_LIST_SHARDS_RETRY_ATTEMPTS)).listShards(any(ListShardsRequest.class));
        }
    }

    @Test
    public void testListShardsResourceNotFoundReturnsEmptyResponse() {
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.supplyAsync(() -> {
            throw ResourceNotFoundException.builder().build();
        });
        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        List<Shard> shards = shardDetector.listShards();

        Assert.assertEquals(0, shards.size());
        verify(client).listShards(any(ListShardsRequest.class));
    }

    @Test
    public void testListShardsTimesOut() throws Exception {
        expectedExceptionRule.expect(RuntimeException.class);
        expectedExceptionRule.expectCause(isA(TimeoutException.class));

        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("Timeout"));

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(mockFuture);

        shardDetector.listShards();
    }

    @Test
    public void testGetShard() {
        final String shardId = String.format(SHARD_ID, 1);

        shardDetector.cachedShardMap(createShardList());

        final Shard shard = shardDetector.shard(shardId);

        assertThat(shard, equalTo(Shard.builder().shardId(shardId).build()));
        verify(client, never()).listShards(any(ListShardsRequest.class));
    }

    @Test
    public void testGetShardEmptyCache() {
        final String shardId = String.format(SHARD_ID, 1);
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.completedFuture(
                ListShardsResponse.builder().shards(createShardList()).build());

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        final Shard shard = shardDetector.shard(shardId);

        assertThat(shard, equalTo(Shard.builder().shardId(shardId).build()));
        verify(client).listShards(any(ListShardsRequest.class));
    }

    @Test
    public void testGetShardNonExistentShard() {
        final String shardId = String.format(SHARD_ID, 5);

        shardDetector.cachedShardMap(createShardList());

        final Shard shard = shardDetector.shard(shardId);

        assertThat(shard, nullValue());
        assertThat(shardDetector.cacheMisses().get(), equalTo(1));
        verify(client, never()).listShards(any(ListShardsRequest.class));
    }

    @Test
    public void testGetShardNewShardForceRefresh() {
        final String shardId = String.format(SHARD_ID, 5);
        final List<Shard> shards = new ArrayList<>(createShardList());
        shards.add(Shard.builder().shardId(shardId).build());

        final CompletableFuture<ListShardsResponse> future = CompletableFuture.completedFuture(
                ListShardsResponse.builder().shards(shards).build());

        shardDetector.cachedShardMap(createShardList());

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        final List<Shard> responses = IntStream.range(0, MAX_CACHE_MISSES_BEFORE_RELOAD + 1)
                .mapToObj(x -> shardDetector.shard(shardId))
                .collect(Collectors.toList());

        IntStream.range(0, MAX_CACHE_MISSES_BEFORE_RELOAD).forEach(x -> {
            assertThat(responses.get(x), nullValue());
        });

        assertThat(
                responses.get(MAX_CACHE_MISSES_BEFORE_RELOAD),
                equalTo(Shard.builder().shardId(shardId).build()));
        verify(client).listShards(any(ListShardsRequest.class));
    }

    @Test
    public void testGetShardNonExistentShardForceRefresh() {
        final String shardId = String.format(SHARD_ID, 5);
        final CompletableFuture<ListShardsResponse> future = CompletableFuture.completedFuture(
                ListShardsResponse.builder().shards(createShardList()).build());

        shardDetector.cachedShardMap(createShardList());

        when(client.listShards(any(ListShardsRequest.class))).thenReturn(future);

        final List<Shard> responses = IntStream.range(0, MAX_CACHE_MISSES_BEFORE_RELOAD + 1)
                .mapToObj(x -> shardDetector.shard(shardId))
                .collect(Collectors.toList());

        responses.forEach(response -> assertThat(response, nullValue()));
        assertThat(shardDetector.cacheMisses().get(), equalTo(0));
        verify(client).listShards(any(ListShardsRequest.class));
    }

    private List<Shard> createShardList() {
        return Arrays.asList(
                Shard.builder().shardId(String.format(SHARD_ID, 0)).build(),
                Shard.builder().shardId(String.format(SHARD_ID, 1)).build(),
                Shard.builder().shardId(String.format(SHARD_ID, 2)).build(),
                Shard.builder().shardId(String.format(SHARD_ID, 3)).build(),
                Shard.builder().shardId(String.format(SHARD_ID, 4)).build());
    }
}
