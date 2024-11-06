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
package software.amazon.kinesis.coordinator.migration;

/**
 * ClientVersion support during upgrade from KCLv2.x to KCLv3.x
 *
 * This enum is persisted in storage, so any changes to it needs to be backward compatible.
 * Reorganizing the values is not backward compatible, also if versions are removed, the corresponding
 * enum value cannot be reused without backward compatibility considerations.
 */
public enum ClientVersion {
    /**
     * This is a transient start state version used during initialization of the Migration State Machine.
     */
    CLIENT_VERSION_INIT,
    /**
     * This version is used during the upgrade of an application from KCLv2.x to KCLv3.x, in this version
     * KCL workers will emit WorkerMetricStats and run KCLv2.x algorithms for leader election and lease
     * assignment. KCL will also monitor for upgrade to KCLv3.x readiness of the worker fleet.
     */
    CLIENT_VERSION_UPGRADE_FROM_2X,
    /**
     * This version is used during rollback from CLIENT_VERSION_UPGRADE_FROM_2X or CLIENT_VERSION_3X_WITH_ROLLBACK,
     * which can only be initiated using a KCL migration tool, when customer wants to revert to KCLv2.x functionality.
     * In this version, KCL will not emit WorkerMetricStats and run KCLv2.x algorithms for leader election
     * and lease assignment. In this version, KCL will monitor for roll-forward scenario where
     * client version is updated to CLIENT_VERSION_UPGRADE_FROM_2X using the migration tool.
     */
    CLIENT_VERSION_2X,
    /**
     * When workers are operating in CLIENT_VERSION_UPGRADE_FROM_2X and when worker fleet is determined to be
     * KCLv3.x ready (when lease table GSI is active and worker-metrics are being emitted by all lease owners)
     * then the leader will initiate the switch to KCLv3.x algorithms for leader election and lease assignment,
     * by using this version and persisting it in the {@link MigrationState} that allows all worker hosts
     * to also flip to KCLv3.x functionality. In this KCL will also monitor for rollback to detect when the
     * customer updates version to CLIENT_VERSION_2X using migration tool, so that it instantly flips back
     * to CLIENT_VERSION_2X.
     */
    CLIENT_VERSION_3X_WITH_ROLLBACK,
    /**
     * A new application starting KCLv3.x or an upgraded application from KCLv2.x after upgrade is successful
     * can use this version to default all KCLv3.x algorithms without any monitor to rollback.
     */
    CLIENT_VERSION_3X;
}
