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

import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

/**
 * This class uses the GetRecordsRetrievalStrategy class to retrieve the next set of records and update the cache. 
 */
public interface GetRecordsRetriever {
    GetRecordsResponse getNextRecords(int maxRecords);
}
