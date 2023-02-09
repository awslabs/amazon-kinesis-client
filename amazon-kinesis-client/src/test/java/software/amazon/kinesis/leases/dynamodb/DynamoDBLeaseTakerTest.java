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
package software.amazon.kinesis.leases.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseTaker;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsScope;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaseTakerTest {

    private static final String WORKER_IDENTIFIER = "foo";
    private static final long LEASE_DURATION_MILLIS = 1000L;

    private DynamoDBLeaseTaker dynamoDBLeaseTaker;

    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private MetricsFactory metricsFactory;
    @Mock
    private Callable<Long> timeProvider;

    @Before
    public void setup() {
        this.dynamoDBLeaseTaker = new DynamoDBLeaseTaker(leaseRefresher, WORKER_IDENTIFIER, LEASE_DURATION_MILLIS, metricsFactory);
    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link DynamoDBLeaseTaker#stringJoin(java.util.Collection, java.lang.String)}.
     */
    @Test
    public final void testStringJoin() {
        List<String> strings = new ArrayList<>();
        
        strings.add("foo");
        Assert.assertEquals("foo", DynamoDBLeaseTaker.stringJoin(strings, ", "));
        
        strings.add("bar");
        Assert.assertEquals("foo, bar", DynamoDBLeaseTaker.stringJoin(strings, ", "));
    }

    @Test
    public void test_computeActiveLeaseCountsByWorker_noAvailableLeases() throws Exception {
        final List<Lease> leases = new ImmutableList.Builder<Lease>()
                .add(createLease(null, "1"))
                .add(createLease("foo", "2"))
                .add(createLease("bar", "3"))
                .add(createLease("baz", "4"))
                .build();
        dynamoDBLeaseTaker.allLeases.putAll(
                leases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));

        when(leaseRefresher.listLeases()).thenReturn(leases);
        when(metricsFactory.createMetrics()).thenReturn(new NullMetricsScope());
        when(timeProvider.call()).thenReturn(1000L);

        final Map<String, Integer> actualOutput = dynamoDBLeaseTaker.computeActiveLeaseCountsByWorker(ImmutableList.of());

        final Map<String, Integer> expectedOutput = new HashMap<>();
        expectedOutput.put("foo", 1);
        expectedOutput.put("bar", 1);
        expectedOutput.put("baz", 1);
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void test_computeActiveLeaseCountsByWorker_withAvailableLeases() throws Exception {
        final List<Lease> leases = new ImmutableList.Builder<Lease>()
                .add(createLease("foo", "2"))
                .add(createLease("bar", "3"))
                .add(createLease("baz", "4"))
                .build();
        dynamoDBLeaseTaker.allLeases.putAll(
                leases.stream().collect(Collectors.toMap(Lease::leaseKey, Function.identity())));

        when(leaseRefresher.listLeases()).thenReturn(leases);
        when(metricsFactory.createMetrics()).thenReturn(new NullMetricsScope());
        when(timeProvider.call()).thenReturn(1000L);

        final Map<String, Integer> actualOutput = dynamoDBLeaseTaker.computeActiveLeaseCountsByWorker(leases);

        final Map<String, Integer> expectedOutput = new HashMap<>();
        expectedOutput.put("foo", 0);
        assertEquals(expectedOutput, actualOutput);
    }

    private Lease createLease(String leaseOwner, String leaseKey) {
        final Lease lease = new Lease();
        lease.checkpoint(new ExtendedSequenceNumber("checkpoint"));
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.leaseCounter(0L);
        lease.leaseOwner(leaseOwner);
        lease.parentShardIds(Collections.singleton("parentShardId"));
        lease.childShardIds(new HashSet<>());
        lease.leaseKey(leaseKey);
        return lease;
    }
}
