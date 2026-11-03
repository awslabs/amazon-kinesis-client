/*
 * Copyright 2026 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.GetLockOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.coordinator.CoordinatorStateDAO;
import software.amazon.kinesis.coordinator.migration.TableMigrationStateMachine;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class DynamoDBLockBasedLeaderDeciderFailoverTest {
    private static final String TABLE = "LeaderFailoverTest";
    // The released 1.4.0 client resets non-owned lookupTime to zero. A valid lease exceeding every
    // nanoTime-derived millisecond value keeps that metadata unexpired regardless of clock origin,
    // reproducing the decider's gate without replacing getLock behavior or waiting for a lease.
    private static final long LEASE_MILLIS = TimeUnit.NANOSECONDS.toMillis(Long.MAX_VALUE) + 1;

    private final List<Worker> workers = new ArrayList<>();
    private AmazonDynamoDBLocal embedded;
    private DynamoDbClient dynamoDb;

    @BeforeEach
    void setup() {
        embedded = DynamoDBEmbedded.create();
        dynamoDb = embedded.dynamoDbClient();
        dynamoDb.createTable(CreateTableRequest.builder()
                .tableName(TABLE)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(KeySchemaElement.builder()
                        .attributeName("key")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("key")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .build());
    }

    @AfterEach
    void cleanup() throws Exception {
        for (Worker worker : workers) {
            worker.client.close();
        }
        embedded.shutdown();
    }

    @Test
    void abandonedLeaderCanBeReplacedRepeatedly() {
        Worker first = worker("first", dynamoDb);
        Worker second = worker("second", dynamoDb);
        Worker third = worker("third", dynamoDb);
        assertTrue(first.isLeader());
        assertFalse(second.isLeader());

        first.expireOwnedLock();
        second.expireObservation();
        assertTrue(second.isLeader());
        assertFalse(first.isLeader());
        assertFalse(third.isLeader());

        second.expireOwnedLock();
        third.expireObservation();
        assertTrue(third.isLeader());
        assertFalse(second.isLeader());
    }

    @Test
    void heartbeatChangesRvnAndRestartsObservationPeriod() {
        Worker leader = worker("leader", dynamoDb);
        Worker follower = worker("follower", dynamoDb);
        assertTrue(leader.isLeader());
        assertFalse(follower.isLeader());
        String oldRvn = follower.observed.get().getRecordVersionNumber();
        follower.expireObservation();

        leader.client.sendHeartbeat(leader.ownedLock());
        assertFalse(follower.isLeader());
        assertNotEquals(oldRvn, follower.observed.get().getRecordVersionNumber());
        assertTrue(leader.isLeader());

        leader.expireOwnedLock();
        follower.expireObservation();
        assertTrue(follower.isLeader());
    }

    @Test
    void gracefulShutdownAllowsImmediateTakeover() {
        Worker leader = worker("leader", dynamoDb);
        Worker follower = worker("follower", dynamoDb);
        assertTrue(leader.isLeader());
        assertFalse(follower.isLeader());

        leader.decider.shutdown();
        assertFalse(leader.isLeader());
        assertTrue(follower.isLeader());
    }

    @Test
    void heartbeatAfterReadRejectsStaleConditionalAcquisition() {
        Worker leader = worker("leader", dynamoDb);
        DynamoDbClient delayed = spy(dynamoDb);
        Worker follower = worker("follower", delayed);
        assertTrue(leader.isLeader());
        assertFalse(follower.isLeader());
        follower.expireObservation();

        AtomicBoolean heartbeated = new AtomicBoolean();
        doAnswer(invocation -> {
                    if (heartbeated.compareAndSet(false, true)) {
                        // The leader advances the RVN after the follower's read, before its write reaches DynamoDB.
                        leader.client.sendHeartbeat(leader.ownedLock());
                    }
                    return invocation.callRealMethod();
                })
                .when(delayed)
                .putItem(any(PutItemRequest.class));

        assertFalse(follower.isLeader());
        assertTrue(heartbeated.get());
        assertTrue(leader.isLeader());
    }

    @Test
    void delayedReadOfOldRvnCannotDisplaceHeartbeatingLeader() {
        Worker leader = worker("leader", dynamoDb);
        DynamoDbClient delayed = spy(dynamoDb);
        Worker follower = worker("follower", delayed);
        assertTrue(leader.isLeader());
        String oldRvn = leader.ownedLock().getRecordVersionNumber();
        AtomicInteger reads = new AtomicInteger();
        doAnswer(invocation -> {
                    GetItemResponse response = (GetItemResponse) invocation.callRealMethod();
                    if (reads.incrementAndGet() == 2) {
                        // Delay the acquisition read's response until after the leader has heartbeated.
                        leader.client.sendHeartbeat(leader.ownedLock());
                    }
                    return response;
                })
                .when(delayed)
                .getItem(any(GetItemRequest.class));

        assertFalse(follower.isLeader());
        assertEquals(oldRvn, follower.observed.get().getRecordVersionNumber());
        follower.expireObservation();
        assertFalse(follower.isLeader());
        assertNotEquals(oldRvn, follower.observed.get().getRecordVersionNumber());
        assertTrue(leader.isLeader());
    }

    @Test
    void simultaneousContendersCannotBothAcquireExpiredLeadership() throws Exception {
        Worker leader = worker("leader", dynamoDb);
        DynamoDbClient contenderDb = spy(dynamoDb);
        Worker first = worker("first", contenderDb);
        Worker second = worker("second", contenderDb);
        assertTrue(leader.isLeader());
        assertFalse(first.isLeader());
        assertFalse(second.isLeader());
        leader.expireOwnedLock();
        first.expireObservation();
        second.expireObservation();

        CyclicBarrier writes = new CyclicBarrier(2);
        doAnswer(invocation -> {
                    writes.await(10, TimeUnit.SECONDS);
                    return invocation.callRealMethod();
                })
                .when(contenderDb)
                .putItem(any(PutItemRequest.class));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> firstResult = executor.submit(first::isLeader);
            Future<Boolean> secondResult = executor.submit(second::isLeader);
            assertNotEquals(firstResult.get(15, TimeUnit.SECONDS), secondResult.get(15, TimeUnit.SECONDS));
            assertFalse(leader.isLeader());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private Worker worker(String id, DynamoDbClient db) {
        Worker worker = new Worker(id, db);
        workers.add(worker);
        return worker;
    }

    private static void expire(LockItem item) {
        // Backdate only this client's local observation; leave the DynamoDB record and RVN untouched.
        item.updateLookUpTime(LockClientUtils.INSTANCE.millisecondTime() - item.getLeaseDuration() - 1);
    }

    private static final class Worker {
        private final String id;
        private final AmazonDynamoDBLockClient client;
        private final DynamoDBLockBasedLeaderDecider decider;
        private final AtomicReference<LockItem> observed = new AtomicReference<>();

        private Worker(String id, DynamoDbClient db) {
            this.id = id;
            client = spy(new AmazonDynamoDBLockClient(AmazonDynamoDBLockClientOptions.builder(db, TABLE)
                    .withPartitionKeyName("key")
                    .withOwnerName(id)
                    .withLeaseDuration(LEASE_MILLIS)
                    .withHeartbeatPeriod(1000L)
                    .withTimeUnit(TimeUnit.MILLISECONDS)
                    .withCreateHeartbeatBackgroundThread(false)
                    .build()));
            doAnswer(invocation -> {
                        @SuppressWarnings("unchecked")
                        Optional<LockItem> result = (Optional<LockItem>) invocation.callRealMethod();
                        result.ifPresent(observed::set);
                        return result;
                    })
                    .when(client)
                    .getLockFromDynamoDB(any(GetLockOptions.class));
            CoordinatorStateDAO dao = mock(CoordinatorStateDAO.class);
            when(dao.getDDBLockClient()).thenReturn(client);
            decider = new DynamoDBLockBasedLeaderDecider(
                    dao, 0L, id, new NullMetricsFactory(), mock(TableMigrationStateMachine.class));
        }

        private boolean isLeader() {
            return decider.isLeader(id);
        }

        private LockItem ownedLock() {
            Optional<LockItem> item = client.getLock(LeaderLock.LEADER_HASH_KEY, Optional.empty());
            assertTrue(item.isPresent());
            assertEquals(id, item.get().getOwnerName());
            return item.get();
        }

        private void expireOwnedLock() {
            expire(ownedLock());
        }

        private void expireObservation() {
            expire(observed.get());
        }
    }
}
