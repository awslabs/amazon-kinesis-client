/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

/**
 * These are the special fields that will be updated only once during the lifetime of the lease.
 * Since these are meta information that will not affect lease ownership or data durability, we allow
 * any elected leader  or worker to set these fields directly without any conditional checks.
 * Note that though HASH_KEY_RANGE will be available during lease initialization in newer versions, we keep this
 * for backfilling while rolling forward to newer versions.
 */
public enum UpdateField {
    CHILD_SHARDS,
    HASH_KEY_RANGE
}
