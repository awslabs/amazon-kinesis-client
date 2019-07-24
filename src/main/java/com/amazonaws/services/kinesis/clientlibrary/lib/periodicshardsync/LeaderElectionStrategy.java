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
package com.amazonaws.services.kinesis.clientlibrary.lib.periodicshardsync;

import java.util.List;
import java.util.Set;

import com.amazonaws.services.kinesis.leases.impl.Lease;

/**
 * Interface for electing leaders and registering listeners interested in being
 * notified on leader election
 *
 * @param <T>
 */
public interface LeaderElectionStrategy<T extends Lease> extends Runnable {
	
    /**
     * @param leases Leases held by workers from which the leaders are chsoen
     * @return Elected set of leaders based on the concrete leader election strategy implementation
     */
    Set<String> electLeaders(List<T> leases);

    Boolean isLeader(String workerId);
}
