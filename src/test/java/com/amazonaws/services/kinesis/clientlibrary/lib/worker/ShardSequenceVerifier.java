/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.Shard;

/**
 * Helper class to verify shard lineage in unit tests that use TestStreamlet.
 * Verifies that parent shard processors were shutdown before child shard processor was initialized.
 */
class ShardSequenceVerifier {

    private static final Log LOG = LogFactory.getLog(ShardSequenceVerifier.class);
    private Map<String, Shard> shardIdToShards = new HashMap<String, Shard>();
    private ConcurrentSkipListSet<String> initializedShards = new ConcurrentSkipListSet<>();
    private ConcurrentSkipListSet<String> shutdownShards = new ConcurrentSkipListSet<>();
    private List<String> validationFailures = Collections.synchronizedList(new ArrayList<String>());

    /**
     * Constructor with the shard list for the stream.
     */
    ShardSequenceVerifier(List<Shard> shardList) {
        for (Shard shard : shardList) {
            shardIdToShards.put(shard.getShardId(), shard);
        }
    }
    
    void registerInitialization(String shardId) {
        List<String> parentShardIds = ShardObjectHelper.getParentShardIds(shardIdToShards.get(shardId));
        for (String parentShardId : parentShardIds) {
            if (initializedShards.contains(parentShardId)) {
                if (!shutdownShards.contains(parentShardId)) {
                    String message = "Parent shard " + parentShardId + " was not shutdown before shard "
                            + shardId + " was initialized.";
                    LOG.error(message);
                    validationFailures.add(message);
                }
            }
        }
        initializedShards.add(shardId);
    }
    
    void registerShutdown(String shardId, ShutdownReason reason) {
        if (reason.equals(ShutdownReason.TERMINATE)) {
            shutdownShards.add(shardId);
        }
    }
    
    void verify() {
        for (String message : validationFailures) {
            LOG.error(message);
        }
        Assert.assertTrue(validationFailures.isEmpty());
    }

}
