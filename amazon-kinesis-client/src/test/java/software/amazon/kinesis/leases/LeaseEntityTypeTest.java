/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

import java.util.Collections;
import java.util.HashSet;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Lease implementing the EntityDAO.Entity interface.
 */
class LeaseEntityTypeTest {

    @Test
    void lease_implementsEntityInterface() {
        final Lease lease = new Lease();
        assertTrue(lease instanceof EntityDAO.Entity, "Lease should implement EntityDAO.Entity");
    }

    @Test
    void getEntityType_returnsLease() {
        final Lease lease = new Lease();
        assertNotNull(lease.getEntityType());
        assertEquals(EntityType.LEASE, lease.getEntityType());
    }

    @Test
    void getEntityType_alwaysReturnsLease_regardlessOfFields() {
        final Lease lease = new Lease();
        lease.leaseKey("shard-001");
        lease.leaseOwner("worker-1");
        lease.leaseCounter(42L);
        lease.ownerSwitchesSinceCheckpoint(3L);
        lease.parentShardIds(Collections.singleton("parent"));
        lease.childShardIds(new HashSet<>());
        lease.checkpoint(new ExtendedSequenceNumber("12345"));

        assertEquals(EntityType.LEASE, lease.getEntityType());
    }

    @Test
    void getEntityType_ddbValue_isLEASE() {
        final Lease lease = new Lease();
        assertEquals("LEASE", lease.getEntityType().getDdbValue());
    }

    @Test
    void getEntityType_isConsistentAcrossMultipleCalls() {
        final Lease lease = new Lease();
        assertEquals(lease.getEntityType(), lease.getEntityType());
        assertEquals(EntityType.LEASE, lease.getEntityType());
    }
}
