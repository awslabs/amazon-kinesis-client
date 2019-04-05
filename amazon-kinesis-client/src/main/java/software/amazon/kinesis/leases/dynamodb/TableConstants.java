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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This class is just a holder for initial lease table IOPs units. This class will be removed in a future release.
 */
@Deprecated
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableConstants {
    public static final long DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY = 10L;
    public static final long DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10L;
}
