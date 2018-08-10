/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
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
