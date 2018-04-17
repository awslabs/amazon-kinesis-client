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

package software.amazon.kinesis.retrieval;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

import java.util.Date;

/**
 *
 */
public interface RetrievalProxy {
    String getShardIterator(ShardIteratorType shardIteratorType, String sequenceNumber, Date timestamp);

    GetRecordsResult getRecords(String shardIterator) throws ResourceNotFoundException, InvalidArgumentException,
            ExpiredIteratorException;
}
