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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;


import junit.framework.Assert;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.lifecycle.ShutdownReason;

/**
 * Helper class to verify shard lineage in unit tests that use TestStreamlet.
 * Verifies that parent shard processors were shutdown before child shard processor was initialized.
 */
@Slf4j
public class ShardSequenceVerifier {
    private Map<String, Shard> shardIdToShards = new HashMap<String, Shard>();
    private ConcurrentSkipListSet<String> initializedShards = new ConcurrentSkipListSet<>();
    private ConcurrentSkipListSet<String> shutdownShards = new ConcurrentSkipListSet<>();
    private List<String> validationFailures = Collections.synchronizedList(new ArrayList<String>());

    /**
     * Constructor with the shard list for the stream.
     */
    public ShardSequenceVerifier(List<Shard> shardList) {
        for (Shard shard : shardList) {
            shardIdToShards.put(shard.shardId(), shard);
        }
    }
    
    public void registerInitialization(String shardId) {
        List<String> parentShardIds = ShardObjectHelper.getParentShardIds(shardIdToShards.get(shardId));
        for (String parentShardId : parentShardIds) {
            if (initializedShards.contains(parentShardId)) {
                if (!shutdownShards.contains(parentShardId)) {
                    String message = "Parent shard " + parentShardId + " was not shutdown before shard "
                            + shardId + " was initialized.";
                    log.error(message);
                    validationFailures.add(message);
                }
            }
        }
        initializedShards.add(shardId);
    }
    
    public void registerShutdown(String shardId, ShutdownReason reason) {
        if (reason.equals(ShutdownReason.SHARD_END)) {
            shutdownShards.add(shardId);
        }
    }
    
    public void verify() {
        for (String message : validationFailures) {
            log.error(message);
        }
        Assert.assertTrue(validationFailures.isEmpty());
    }

}
