# Changelog

### Latest Release (1.14.8 - Feb 24, 2021)
* [Bump log4j-core from 2.17.0 to 2.17.1](https://github.com/awslabs/amazon-kinesis-client/commit/94b138a9d9a502ee0f4f000bb0efd2766ebadc37)
* [Bump protobuf-java from 3.19.1 to 3.19.4](https://github.com/awslabs/amazon-kinesis-client/commit/a809b12c43c57a3d6ad3827feb60e4322614259c)
* [Bump maven-compiler-plugin from 3.8.1 to 3.10.0](https://github.com/awslabs/amazon-kinesis-client/commit/37b5d7b9a1ccad483469ef542a6a7237462b14f2)

### Release (1.14.7 - Dec 22, 2021)
* [#881](https://github.com/awslabs/amazon-kinesis-client/pull/881) Update log4j test dependency from 2.16.0 to 2.17.0 and some other dependencies

### Release (1.14.6 - Dec 15, 2021)
* [#876](https://github.com/awslabs/amazon-kinesis-client/pull/876) Update log4j test dependency from 2.15.0 to 2.16.0

### Release (1.14.5 - Dec 10, 2021)
* [#872](https://github.com/awslabs/amazon-kinesis-client/pull/872) Update log4j test dependency from 1.2.17 to 2.15.0
* [#873](https://github.com/awslabs/amazon-kinesis-client/pull/873) Upgrading version of AWS Java SDK to 1.12.128

### Release (1.14.4 - June 14, 2021)
* [Milestone#61](https://github.com/awslabs/amazon-kinesis-client/milestone/61)
* [#816](https://github.com/awslabs/amazon-kinesis-client/pull/816) Updated the Worker shutdown logic to make sure that the `LeaseCleanupManager` also terminates all the threads that it has started.
* [#821](https://github.com/awslabs/amazon-kinesis-client/pull/821) Upgrading version of AWS Java SDK to 1.12.3

### Release (1.14.3 - May 3, 2021)
* [Milestone#60](https://github.com/awslabs/amazon-kinesis-client/milestone/60)
* [#811](https://github.com/awslabs/amazon-kinesis-client/pull/811) Fixing a bug in `KinesisProxy` that can lead to undetermined behavior during partial failures.
* [#811](https://github.com/awslabs/amazon-kinesis-client/pull/811) Adding guardrails to handle duplicate shards from the service.

## Release (1.14.2 - February 24, 2021)
* [Milestone#57](https://github.com/awslabs/amazon-kinesis-client/milestone/57)
* [#790](https://github.com/awslabs/amazon-kinesis-client/pull/790) Fixing a bug that caused paginated `ListShards` calls with the `ShardFilter` parameter to fail when the lease table was being initialized.

## Release (1.14.1 - January 27, 2021)
* [Milestone#56](https://github.com/awslabs/amazon-kinesis-client/milestone/56)

* Fix for cross DDB table interference when multiple KCL applications are run in same JVM.
* Fix and guards to avoid potential checkpoint rewind during shard end, which may block children shard processing.
* Fix for thread cycle wastage on InitializeTask for deleted shard.
* Improved logging in LeaseCleanupManager that would indicate why certain shards are not cleaned up from the lease table.


## Release (1.14.0 - August 17, 2020)

* [Milestone#50](https://github.com/awslabs/amazon-kinesis-client/milestone/50)

* Behavior of shard synchronization is moving from each worker independently learning about all existing shards to workers only discovering the children of shards that each worker owns. This optimizes memory usage, lease table IOPS usage, and number of calls made to kinesis for streams with high shard counts and/or frequent resharding.
* When bootstrapping an empty lease table, KCL utilizes the ListShard API's filtering option (the ShardFilter optional request parameter) to retrieve and create leases only for a snapshot of shards open at the time specified by the ShardFilter parameter. The ShardFilter parameter enables you to filter out the response of the ListShards API, using the Type parameter. KCL uses the Type filter parameter and the following of its valid values to identify and return a snapshot of open shards that might require new leases.
	* Currently, the following shard filters are supported:
		* `AT_TRIM_HORIZON` - the response includes all the shards that were open at `TRIM_HORIZON`.
		* `AT_LATEST` - the response includes only the currently open shards of the data stream.
	    * `AT_TIMESTAMP` - the response includes all shards whose start timestamp is less than or equal to the given timestamp and end timestamp is greater than or equal to the given timestamp or still open.
	* `ShardFilter` is used when creating leases for an empty lease table to initialize leases for a snapshot of shards specified at `KinesisClientLibConfiguration#initialPositionInStreamExtended`.
	* For more information about ShardFilter, see the [official AWS documentation on ShardFilter](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ShardFilter.html).

* Introducing support for the `ChildShards` response of the `GetRecords` API to perform lease/shard synchronization that happens at `SHARD_END` for closed shards, allowing a KCL worker to only create leases for the child shards of the shard it finished processing.
	* For KCL 1.x applications, this uses the `ChildShards` response of the `GetRecords` API.
	* For more information, see the official AWS Documentation on [GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) and [ChildShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ChildShard.html).

* KCL now also performs additional periodic shard/lease scans in order to identify any potential holes in the lease table to ensure the complete hash range of the stream is being processed and create leases for them if required. When `KinesisClientLibConfiguration#shardSyncStrategyType` is set to `ShardSyncStrategyType.SHARD_END`, `PeriodicShardSyncManager#leasesRecoveryAuditorInconsistencyConfidenceThreshold` will be used to determine the threshold for number of consecutive scans containing holes in the lease table after which to enforce a shard sync. When `KinesisClientLibConfiguration#shardSyncStrategyType` is set to `ShardSyncStrategyType.PERIODIC`, `leasesRecoveryAuditorInconsistencyConfidenceThreshold` is ignored.
	* New configuration options are available to configure `PeriodicShardSyncManager` in `KinesisClientLibConfiguration`

	| Name                                                  | Default | Description                                                                                                                                                                                                                                                                                       |
	| ----------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
	| leasesRecoveryAuditorInconsistencyConfidenceThreshold | 3       | Confidence threshold for the periodic auditor job to determine if leases for a stream in the lease table is inconsistent. If the auditor finds same set of inconsistencies consecutively for a stream for this many times, then it would trigger a shard sync. Only used for `ShardSyncStrategyType.SHARD_END`. |

	* New CloudWatch metrics are also now emitted to monitor the health of `PeriodicShardSyncManager`:

	| Name                        | Description                                            |
	| --------------------------- | ------------------------------------------------------ |
	| NumStreamsWithPartialLeases | Number of streams that had holes in their hash ranges. |
	| NumStreamsToSync            | Number of streams which underwent a full shard sync.   |

* Introducing deferred lease cleanup. Leases will be deleted asynchronously by `LeaseCleanupManager` upon reaching `SHARD_END`, when a shard has either expired past the stream’s retention period or been closed as the result of a resharding operation.
	* New configuration options are available to configure `LeaseCleanupManager`.

	| Name                                | Default    | Description                                                                                               |
	| ----------------------------------- | ---------- | --------------------------------------------------------------------------------------------------------- |
	| leaseCleanupIntervalMillis          | 1 minute   | Interval at which to run lease cleanup thread.                                                            |
	| completedLeaseCleanupIntervalMillis | 5 minutes  | Interval at which to check if a lease is completed or not.                                                |
	| garbageLeaseCleanupIntervalMillis   | 30 minutes | Interval at which to check if a lease is garbage (i.e trimmed past the stream's retention period) or not. |

* Including an optimization to `KinesisShardSyncer` to only create leases for one layer of shards.
* Changing default shard prioritization strategy to be `NoOpShardPrioritization` to allow prioritization of completed shards. Customers who are upgrading to this version and are reading from `TRIM_HORIZON` should continue using `ParentsFirstShardPrioritization` while upgrading.
* Upgrading version of AWS SDK to 1.11.844.
* [#719](https://github.com/awslabs/amazon-kinesis-client/pull/719) Upgrading version of Google Protobuf to 3.11.4.
* [#712](https://github.com/awslabs/amazon-kinesis-client/pull/712) Allowing KCL to consider lease tables in `UPDATING` healthy.

## Release 1.13.3 (1.13.3 March 2, 2020)
[Milestone#49] (https://github.com/awslabs/amazon-kinesis-client/milestone/49）
* Refactoring shard closure verification performed by ShutdownTask.
  * [PR #684] (https://github.com/awslabs/amazon-kinesis-client/pull/684)
* Fixing the bug in ShardSyncTaskManager to resolve the issue of new shards not being processed after resharding.
  * [PR #694] (https://github.com/awslabs/amazon-kinesis-client/pull/694)

## Release 1.13.2 （Janurary 13, 2020）
* Adding backward compatible constructors that use the default DDB Billing Mode (#673)
  * [PR #673](https://github.com/awslabs/amazon-kinesis-client/pull/673)

## Release 1.13.1 (December 31, 2019)
[Milestone#44](https://github.com/awslabs/amazon-kinesis-client/milestone/44)
* Adding BillingMode Support to KCL 1.x. This enables the customer to specify if they want provisioned capacity for DDB, or pay per request.
  * [PR #656](https://github.com/awslabs/amazon-kinesis-client/pull/656)
* Ensure ShardSyncTask invocation from ShardSyncTaskManager for pending ShardEnd events.
  * [PR #659](https://github.com/awslabs/amazon-kinesis-client/pull/659)
* Fix the LeaseManagementIntegrationTest failure.
  * [PR #670](https://github.com/awslabs/amazon-kinesis-client/pull/670)

## Release 1.13.0 (November 5, 2019)
[Milestone#42](https://github.com/awslabs/amazon-kinesis-client/milestone/42)
* Handling completed and blocked tasks better during graceful shutdown
  * [PR #640](https://github.com/awslabs/amazon-kinesis-client/pull/640)

## Release 1.12.0 (October 17, 2019)
[Milestone#41](https://github.com/awslabs/amazon-kinesis-client/milestone/41)
* Adding logging around shard end codepaths.
  * [PR #585](https://github.com/awslabs/amazon-kinesis-client/pull/585)
* Updating checkpointing failure message to refer to javadocs.
  * [PR #590](https://github.com/awslabs/amazon-kinesis-client/pull/590)
* Updating Sonatype to dedicated AWS endpoint.
  * [PR #618](https://github.com/awslabs/amazon-kinesis-client/pull/618)
* Introducing a validation step to verify if ShardEnd is reached, to prevent shard consumer stuck scenarios in the event of malformed response from service.
  * [PR #623](https://github.com/awslabs/amazon-kinesis-client/pull/623)
* Updating AWS SDK to 1.11.655
  * [PR #626](https://github.com/awslabs/amazon-kinesis-client/pull/626)

## Release 1.11.2 (August 15, 2019)
[Milestone#35](https://github.com/awslabs/amazon-kinesis-client/milestone/35)
* Added support for metrics emission in `PeriodicShardSyncer`.
  * [PR #592](https://github.com/awslabs/amazon-kinesis-client/pull/592)

## Release 1.11.1 (August 9, 2019)
[Milestone#34](https://github.com/awslabs/amazon-kinesis-client/milestone/34)
* Updated the version of the AWS Java SDK to 1.11.603.
  * [PR #587](https://github.com/awslabs/amazon-kinesis-client/pull/587)
* Added logging to `KinesisDataFetcher` when reaching the end of a shard due to a null next iterator.
  * [PR #585](https://github.com/awslabs/amazon-kinesis-client/pull/585)

## Release 1.11.0 (August 7, 2019)
[Milestone#33](https://github.com/awslabs/amazon-kinesis-client/milestone/33)
* Improved exception handling and logging in `KinesisClientLibLeaseCoordinator` to avoid `NullPointerExceptions` when no leases are found.
  * [PR #558](https://github.com/awslabs/amazon-kinesis-client/pull/558)
* Introducing optional new periodic shard sync strategy to perform shard discovery and lease cleanup on a single worker.
  * [PR #579](https://github.com/awslabs/amazon-kinesis-client/pull/579)

## Release 1.10.0 (April 8, 2019)
[Milestone#31](https://github.com/awslabs/amazon-kinesis-client/milestone/31)
* Updated License to [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
  * [PR#522](https://github.com/awslabs/amazon-kinesis-client/pull/522)	
## Release 1.9.3 (October 30, 2018)
* Upgraded Apache Commons Lang from 2.6 to 3.7.  
  * [Issue #370](https://github.com/awslabs/amazon-kinesis-client/issues/370)
  * [PR #406](https://github.com/awslabs/amazon-kinesis-client/pull/406)
* Upgraded Google Guava from 10.0 to 26.0-jre.  
  * [Issue #416](https://github.com/awslabs/amazon-kinesis-client/issues/416)
  * [PR #421](https://github.com/awslabs/amazon-kinesis-client/pull/421)

## Release 1.9.2 (September 4, 2018)
* Allow use of Immutable Clients  
  * [Issue #280](https://github.com/awslabs/amazon-kinesis-client/issues/280)
  * [PR #305](https://github.com/awslabs/amazon-kinesis-client/pull/305)
* Allow the use of `AT_TIMESTAMP` for MultiLang Daemon Clients.  
  * [Issue #341](https://github.com/awslabs/amazon-kinesis-client/issues/341)
  * [PR #342](https://github.com/awslabs/amazon-kinesis-client/pull/342)
* Update the cache for `KinesisProxy#getShard` on cache misses.  
  * [PR #344](https://github.com/awslabs/amazon-kinesis-client/pull/344)
* Changed release process to use a standard process.  
  * [PR #389](https://github.com/awslabs/amazon-kinesis-client/pull/389)
* Removed tests that expected a null region response for unknown regions.  
  * [PR #346](https://github.com/awslabs/amazon-kinesis-client/pull/346)
* Updated the version of the AWS Java SDK to 1.11.400  

## Release 1.9.1 (April 30, 2018)
* Added the ability to create a prepared checkpoint when at `SHARD_END`.
  * [PR #301](https://github.com/awslabs/amazon-kinesis-client/pull/301)
* Added the ability to subscribe to worker state change events.  
  * [PR #291](https://github.com/awslabs/amazon-kinesis-client/pull/291)
* Added support for custom lease managers.  
  A custom `LeaseManager` can be provided to `Worker.Builder` that will be used to provide lease services. 
  This makes it possible to implement custom lease management systems in addition to the default DynamoDB system.  
  * [PR #297](https://github.com/awslabs/amazon-kinesis-client/pull/297)
* Updated the version of the AWS Java SDK to 1.11.219

## Release 1.9.0 (February 6, 2018)
* Introducing support for ListShards API. This API is used in place of DescribeStream API to provide more throughput during ShardSyncTask. Please consult the [AWS Documentation for ListShards](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html) for more information.
  * ListShards supports higher call rate, which should reduce instances of throttling when attempting to synchronize the shard list.
  * __WARNING: `ListShards` is a new API, and may require updating any explicit IAM policies__
  * Added configuration parameters for ListShards usage
  
  | Name | Default | Description |
  | ---- | ------- | ----------- |
  | [listShardsBackoffTimeInMillis](https://github.com/awslabs/amazon-kinesis-client/blob/3ae916c5fcdccd6b835c86ba7f6f53dd5b4c8b04/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1412) | 1500 ms | This is the default backoff time between 2 ListShards calls when throttled. |
  | [listShardsRetryAttempts](https://github.com/awslabs/amazon-kinesis-client/blob/3ae916c5fcdccd6b835c86ba7f6f53dd5b4c8b04/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1423) | 50 | This is the maximum number of times the KinesisProxy will retry to make ListShards calls on being throttled. |
  
* Updating the version of AWS Java SDK to 1.11.272.
  * Version 1.11.272 is now the minimum support version of the SDK.
* Deprecating the following methods, and classes. These methods, and classes will be removed in a future release.
  * Deprecated [IKinesisProxy#getStreamInfo](https://github.com/awslabs/amazon-kinesis-client/blob/3ae916c5fcdccd6b835c86ba7f6f53dd5b4c8b04/src/main/java/com/amazonaws/services/kinesis/clientlibrary/proxies/IKinesisProxy.java#L48-L62).
  * Deprecated [IKinesisProxyFactory](https://github.com/awslabs/amazon-kinesis-client/blob/3ae916c5fcdccd6b835c86ba7f6f53dd5b4c8b04/src/main/java/com/amazonaws/services/kinesis/clientlibrary/proxies/IKinesisProxyFactory.java).
  * Deprecated [KinesisProxyFactory](https://github.com/awslabs/amazon-kinesis-client/blob/3ae916c5fcdccd6b835c86ba7f6f53dd5b4c8b04/src/main/java/com/amazonaws/services/kinesis/clientlibrary/proxies/KinesisProxyFactory.java).
  * Deprecated certain [KinesisProxy](https://github.com/awslabs/amazon-kinesis-client/blob/3ae916c5fcdccd6b835c86ba7f6f53dd5b4c8b04/src/main/java/com/amazonaws/services/kinesis/clientlibrary/proxies/KinesisProxy.java) constructors.
    * [PR #293](https://github.com/awslabs/amazon-kinesis-client/pull/293)

## Release 1.8.10
* Allow providing a custom IKinesisProxy implementation.
  * [PR #274](https://github.com/awslabs/amazon-kinesis-client/pull/274)
* Checkpointing on a different thread should no longer emit a warning about NullMetricsScope.
  * [PR #284](https://github.com/awslabs/amazon-kinesis-client/pull/284)
  * [Issue #48](https://github.com/awslabs/amazon-kinesis-client/issues/48)
* Upgraded the AWS Java SDK to version 1.11.271
  * [PR #287](https://github.com/awslabs/amazon-kinesis-client/pull/287)

## Release 1.8.9
* Allow disabling check for the case where a child shard has an open parent shard.  
  There is a race condition where it's possible for the a parent shard to appear open, while having child shards. This check can now be disabled by setting [`ignoreUnexpectedChildShards`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1037) to true.
  * [PR #240](https://github.com/awslabs/amazon-kinesis-client/pull/240)
  * [Issue #210](https://github.com/awslabs/amazon-kinesis-client/issues/210)
* Upgraded the AWS SDK for Java to 1.11.261
  * [PR #281](https://github.com/awslabs/amazon-kinesis-client/pull/281)

## Release 1.8.8
* Fixed issues with leases losses due to `ExpiredIteratorException` in `PrefetchGetRecordsCache` and `AsynchronousFetchingStrategy`.  
  PrefetchGetRecordsCache will request for a new iterator and start fetching data again.  
  * [PR#263](https://github.com/awslabs/amazon-kinesis-client/pull/263)
* Added warning message for long running tasks.  
  Logging long running tasks can be enabled by setting the following configuration property:
  
  | Name | Default | Description |
  | ---- | ------- | ----------- |
  | [`logWarningForTaskAfterMillis`](https://github.com/awslabs/amazon-kinesis-client/blob/3de901ea9327370ed732af86c4d4999c8d99541c/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1367) | Not set | Milliseconds after which the logger will log a warning message for the long running task |
  
  * [PR#259](https://github.com/awslabs/amazon-kinesis-client/pull/259)
* Handling spurious lease renewal failures gracefully.  
  Added better handling of DynamoDB failures when updating leases.  These failures would occur when a request to DynamoDB appeared to fail, but was actually successful.  
  * [PR#247](https://github.com/awslabs/amazon-kinesis-client/pull/247)
* ShutdownTask gets retried if the previous attempt on the ShutdownTask fails.
  * [PR#267](https://github.com/awslabs/amazon-kinesis-client/pull/267)
* Fix for using maxRecords from `KinesisClientLibConfiguration` in `GetRecordsCache` for fetching records.
  * [PR#264](https://github.com/awslabs/amazon-kinesis-client/pull/264)

## Release 1.8.7
* Don't add a delay for synchronous requests to Kinesis  
  Removes a delay that had been added for synchronous `GetRecords` calls to Kinesis. 
  * [PR #256](https://github.com/awslabs/amazon-kinesis-client/pull/256)

## Release 1.8.6
* Add prefetching of records from Kinesis  
  Prefetching will retrieve and queue additional records from Kinesis while the application is processing existing records.  
  Prefetching can be enabled by setting [`dataFetchingStrategy`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1317) to `PREFETCH_CACHED`. Once enabled an additional fetching thread will be started to retrieve records from Kinesis. Retrieved records will be held in a queue until the application is ready to process them.  
  Pre-fetching supports the following configuration values:  
  
  | Name | Default | Description |
  | ---- | ------- | ----------- |
  | [`dataFetchingStrategy`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1317) | `DEFAULT` | Which data fetching strategy to use |
  | [`maxPendingProcessRecordsInput`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1296) | 3 | The maximum number of process records input that can be queued |
  | [`maxCacheByteSize`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1307) | 8 MiB | The maximum number of bytes that can be queued |
  | [`maxRecordsCount`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1326) | 30,000 | The maximum number of records that can be queued |
  | [`idleMillisBetweenCalls`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L1353) | 1,500 ms | The amount of time to wait between calls to Kinesis |
  
  * [PR #246](https://github.com/awslabs/amazon-kinesis-client/pull/246)

## Release 1.8.5 (September 26, 2017)
* Only advance the shard iterator for the accepted response.  
  This fixes a race condition in the `KinesisDataFetcher` when it's being used to make asynchronous requests.  The shard iterator is now only advanced when the retriever calls `DataFetcherResult#accept()`.
  * [PR #230](https://github.com/awslabs/amazon-kinesis-client/pull/230)
  * [Issue #231](https://github.com/awslabs/amazon-kinesis-client/issues/231)

## Release 1.8.4 (September 22, 2017)
* Create a new completion service for each request.  
  This ensures that canceled tasks are discarded.  This will prevent a cancellation exception causing issues processing records.
  * [PR #227](https://github.com/awslabs/amazon-kinesis-client/pull/227)
  * [Issue #226](https://github.com/awslabs/amazon-kinesis-client/issues/226)

## Release 1.8.3 (September 22, 2017)
* Call shutdown on the retriever when the record processor is being shutdown  
  This fixes a bug that could leak threads if using the [`AsynchronousGetRecordsRetrievalStrategy`](https://github.com/awslabs/amazon-kinesis-client/blob/9a82b6bd05b3c9c5f8581af007141fa6d5f0fc4e/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/AsynchronousGetRecordsRetrievalStrategy.java#L42) is being used.  
  The asynchronous retriever is only used when [`KinesisClientLibConfiguration#retryGetRecordsInSeconds`](https://github.com/awslabs/amazon-kinesis-client/blob/01d2688bc6e68fd3fe5cb698cb65299d67ac930d/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L227), and [`KinesisClientLibConfiguration#maxGetRecordsThreadPool`](https://github.com/awslabs/amazon-kinesis-client/blob/01d2688bc6e68fd3fe5cb698cb65299d67ac930d/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/KinesisClientLibConfiguration.java#L230) are set.
  * [PR #222](https://github.com/awslabs/amazon-kinesis-client/pull/222)

## Release 1.8.2 (September 20, 2017)
* Add support for two phase checkpoints  
  Applications can now set a pending checkpoint, before completing the checkpoint operation. Once the application has completed its checkpoint steps, the final checkpoint will clear the pending checkpoint.  
  Should the checkpoint fail the attempted sequence number is provided in the [`InitializationInput#getPendingCheckpointSequenceNumber`](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/types/InitializationInput.java#L81) otherwise the value will be null.
  * [PR #188](https://github.com/awslabs/amazon-kinesis-client/pull/188)
* Support timeouts, and retry for GetRecords calls.  
  Applications can now set timeouts for GetRecord calls to Kinesis.  As part of setting the timeout, the application must also provide a thread pool size for concurrent requests.
  * [PR #214](https://github.com/awslabs/amazon-kinesis-client/pull/214)
* Notification when the lease table is throttled  
  When writes, or reads, to the lease table are throttled a warning will be emitted.  If you're seeing this warning you should increase the IOPs for your lease table to prevent processing delays.
  * [PR #212](https://github.com/awslabs/amazon-kinesis-client/pull/212)
* Support configuring the graceful shutdown timeout for MultiLang Clients  
  This adds support for setting the timeout that the Java process will wait for the MutliLang client to complete graceful shutdown.  The timeout can be configured by adding `shutdownGraceMillis` to the properties file set to the number of milliseconds to wait.
  * [PR #204](https://github.com/awslabs/amazon-kinesis-client/pull/204)

## Release 1.8.1 (August 2, 2017)
* Support timeouts for calls to the MultiLang Daemon
  This adds support for setting a timeout when dispatching records to the client record processor. If the record processor doesn't respond within the timeout the parent Java process will be terminated. This is a temporary fix to handle cases where the KCL becomes blocked while waiting for a client record processor.
  The timeout for the this can be set by adding `timeoutInSeconds = <timeout value>`. The default for this is no timeout.  
  __Setting this can cause the KCL to exit suddenly, before using this ensure that you have an automated restart for your application__
  * [PR #195](https://github.com/awslabs/amazon-kinesis-client/pull/195)
  * [Issue #185](https://github.com/awslabs/amazon-kinesis-client/issues/185)

## Release 1.8.0 (July 25, 2017)
* Execute graceful shutdown on its own thread
  * [PR #191](https://github.com/awslabs/amazon-kinesis-client/pull/191)
  * [Issue #167](https://github.com/awslabs/amazon-kinesis-client/issues/167)
* Added support for controlling the size of the lease renewer thread pool
  * [PR #177](https://github.com/awslabs/amazon-kinesis-client/pull/177)
  * [Issue #171](https://github.com/awslabs/amazon-kinesis-client/issues/171)
* Require Java 8 and later  
  __Java 8 is now required for versions 1.8.0 of the amazon-kinesis-client and later.__
  * [PR #176](https://github.com/awslabs/amazon-kinesis-client/issues/176)

## Release 1.7.6 (June 21, 2017)
* Added support for graceful shutdown in MultiLang Clients
  * [PR #174](https://github.com/awslabs/amazon-kinesis-client/pull/174)
  * [PR #182](https://github.com/awslabs/amazon-kinesis-client/pull/182)
* Updated documentation for `v2.IRecordProcessor#shutdown`, and `KinesisClientLibConfiguration#idleTimeBetweenReadsMillis`
  * [PR #170](https://github.com/awslabs/amazon-kinesis-client/pull/170)
* Updated to version 1.11.151 of the AWS Java SDK
  * [PR #183](https://github.com/awslabs/amazon-kinesis-client/pull/183)

## Release 1.7.5 (April 7, 2017)
* Correctly handle throttling for DescribeStream, and save accumulated progress from individual calls.
  * [PR #152](https://github.com/awslabs/amazon-kinesis-client/pull/152)
* Upgrade to version 1.11.115 of the AWS Java SDK
  * [PR #155](https://github.com/awslabs/amazon-kinesis-client/pull/155)
  
## Release 1.7.4 (February 27, 2017)
* Fixed an issue building JavaDoc for Java 8.
  * [Issue #18](https://github.com/awslabs/amazon-kinesis-client/issues/18)
  * [PR #141](https://github.com/awslabs/amazon-kinesis-client/pull/141)
* Reduce Throttling Messages to WARN, unless throttling occurs 6 times consecutively.
  * [Issue #4](https://github.com/awslabs/amazon-kinesis-client/issues/4)
  * [PR #140](https://github.com/awslabs/amazon-kinesis-client/pull/140)
* Fixed two bugs occurring in requestShutdown.
  * Fixed a bug that prevented the worker from shutting down, via requestShutdown, when no leases were held.
    * [Issue #128](https://github.com/awslabs/amazon-kinesis-client/issues/128)
  * Fixed a bug that could trigger a NullPointerException if leases changed during requestShutdown.
    * [Issue #129](https://github.com/awslabs/amazon-kinesis-client/issues/129)
  * [PR #139](https://github.com/awslabs/amazon-kinesis-client/pull/139)
* Upgraded the AWS SDK Version to 1.11.91
  * [PR #138](https://github.com/awslabs/amazon-kinesis-client/pull/138)
* Use an executor returned from `ExecutorService.newFixedThreadPool` instead of constructing it by hand.
  * [PR #135](https://github.com/awslabs/amazon-kinesis-client/pull/135)
* Correctly initialize DynamoDB client, when endpoint is explicitly set.
  * [PR #142](https://github.com/awslabs/amazon-kinesis-client/pull/142)

## Release 1.7.3 (January 9, 2017)
* Upgrade to the newest AWS Java SDK.
  * [Amazon Kinesis Client Issue #27](https://github.com/awslabs/amazon-kinesis-client-python/issues/27)
  * [PR #126](https://github.com/awslabs/amazon-kinesis-client/pull/126)
  * [PR #125](https://github.com/awslabs/amazon-kinesis-client/pull/125)
* Added a direct dependency on commons-logging.
  * [Issue #123](https://github.com/awslabs/amazon-kinesis-client/issues/123)
  * [PR #124](https://github.com/awslabs/amazon-kinesis-client/pull/124)
* Make ShardInfo public to allow for custom ShardPrioritization strategies.
  * [Issue #120](https://github.com/awslabs/amazon-kinesis-client/issues/120)
  * [PR #127](https://github.com/awslabs/amazon-kinesis-client/pull/127)

## Release 1.7.2 (November 7, 2016)
* MultiLangDaemon Feature Updates
  The MultiLangDaemon has been upgraded to use the v2 interfaces, which allows access to enhanced checkpointing, and more information during record processor initialization. The MultiLangDaemon clients must be updated before they can take advantage of these new features.
  
## Release 1.7.1 (November 3, 2016)
* General
  * Allow disabling shard synchronization at startup.
    * Applications can disable shard synchronization at startup.  Disabling shard synchronization can application startup times for very large streams.
    * [PR #102](https://github.com/awslabs/amazon-kinesis-client/pull/102)
  * Applications can now request a graceful shutdown, and record processors that implement the IShutdownNotificationAware will be given a chance to checkpoint before being shutdown.
    * This adds a [new interface](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/interfaces/v2/IShutdownNotificationAware.java), and a [new method on Worker](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/clientlibrary/lib/worker/Worker.java#L539).
    * [PR #109](https://github.com/awslabs/amazon-kinesis-client/pull/109)
    * Solves [Issue #79](https://github.com/awslabs/amazon-kinesis-client/issues/79)
* MultiLangDaemon
  * Applications can now use credential provides that accept string parameters.
    * [PR #99](https://github.com/awslabs/amazon-kinesis-client/pull/99)
  * Applications can now use different credentials for each service.
    * [PR #111](https://github.com/awslabs/amazon-kinesis-client/pull/111)

## Release 1.7.0 (August 22, 2016)
* Add support for time based iterators ([See GetShardIterator Documentation](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html))
  * [PR #94](https://github.com/awslabs/amazon-kinesis-client/pull/94)
  The `KinesisClientLibConfiguration` now supports providing an initial time stamp position.
  * This position is only used if there is no current checkpoint for the shard.
  * This setting cannot be used with DynamoDB Streams
  Resolves [Issue #88](https://github.com/awslabs/amazon-kinesis-client/issues/88)
* Allow Prioritization of Parent Shards for Task Assignment
  * [PR #95](https://github.com/awslabs/amazon-kinesis-client/pull/95)
  The `KinesisClientLibconfiguration` now supports providing a `ShardPrioritization` strategy.  This strategy controls how the `Worker` determines which `ShardConsumer` to call next.  This can improve processing for streams that split often, such as DynamoDB Streams.
* Remove direct dependency on `aws-java-sdk-core`, to allow independent versioning.
  * [PR #92](https://github.com/awslabs/amazon-kinesis-client/pull/92)
  **You may need to add a direct dependency on aws-java-sdk-core if other dependencies include an older version.**

## Release 1.6.5 (July 25, 2016)
* Change LeaseManager to call DescribeTable before attempting to create the lease table.
  * [Issue #36](https://github.com/awslabs/amazon-kinesis-client/issues/36)
  * [PR #41](https://github.com/awslabs/amazon-kinesis-client/pull/41)
  * [PR #67](https://github.com/awslabs/amazon-kinesis-client/pull/67)
* Allow DynamoDB lease table name to be specified
  * [PR #61](https://github.com/awslabs/amazon-kinesis-client/pull/61)
* Add approximateArrivalTimestamp for JsonFriendlyRecord
  * [PR #86](https://github.com/awslabs/amazon-kinesis-client/pull/86)
* Shutdown lease renewal thread pool on exit.
  * [PR #84](https://github.com/awslabs/amazon-kinesis-client/pull/84)
* Wait for CloudWatch publishing thread to finish before exiting.
  * [PR #82](https://github.com/awslabs/amazon-kinesis-client/pull/82)
* Added unit, and integration tests for the library.

## Release 1.6.4 (July 6, 2016)
* Upgrade to AWS SDK for Java 1.11.14
  * [Issue #74](https://github.com/awslabs/amazon-kinesis-client/issues/74)
  * [Issue #73](https://github.com/awslabs/amazon-kinesis-client/issues/73)
* **Maven Artifact Signing Change** 
  * Artifacts are now signed by the identity `Amazon Kinesis Tools <amazon-kinesis-tools@amazon.com>`

## Release 1.6.3 (May 12, 2016)
* Fix format exception caused by DEBUG log in LeaseTaker [Issue # 68](https://github.com/awslabs/amazon-kinesis-client/issues/68)

## Release 1.6.2 (March 23, 2016)
* Support for specifying max leases per worker and max leases to steal at a time.
* Support for specifying initial DynamoDB table read and write capacity.
* Support for parallel lease renewal.
* Support for graceful worker shutdown.
* Change DefaultCWMetricsPublisher log level to debug. [PR # 49](https://github.com/awslabs/amazon-kinesis-client/pull/49)
* Avoid NPE in MLD record processor shutdown if record processor was not initialized. [Issue # 29](https://github.com/awslabs/amazon-kinesis-client/issues/29)

## Release 1.6.1 (September 23, 2015)
* Expose [approximateArrivalTimestamp](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) for Records in processRecords API call.

## Release 1.6.0 (July 31, 2015)
* Restores compatibility with [dynamodb-streams-kinesis-adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) (which was broken in 1.4.0).

## Release 1.5.1 (July 20, 2015)
* KCL maven artifact 1.5.0 does not work with JDK 7. This release addresses this issue.

## Release 1.5.0 (July 9, 2015)
* **[Metrics Enhancements][kinesis-guide-monitoring-with-kcl]**
	* Support metrics level and dimension configurations to control CloudWatch metrics emitted by the KCL.
	* Add new metrics that track time spent in record processor methods.
	* Disable WorkerIdentifier dimension by default.
* **Exception Reporting** &mdash; Do not silently ignore exceptions in ShardConsumer.
* **AWS SDK Component Dependencies** &mdash; Depend only on AWS SDK components that are used.

## Release 1.4.0 (June 2, 2015)
* Integration with the **[Kinesis Producer Library (KPL)][kinesis-guide-kpl]**
	* Automatically de-aggregate records put into the Kinesis stream using the KPL.
	* Support checkpointing at the individual user record level when multiple user records are aggregated into one Kinesis record using the KPL.

 See [Consumer De-aggregation with the KCL][kinesis-guide-consumer-deaggregation] for details.

## Release 1.3.0 (May 22, 2015)
* A new metric called "MillisBehindLatest", which tracks how far consumers are from real time, is now uploaded to CloudWatch.

## Release 1.2.1 (January 26, 2015)
* **MultiLangDaemon** &mdash; Changes to the MultiLangDaemon to make it easier to provide a custom worker.

## Release 1.2 (October 21, 2014)
* **Multi-Language Support** &mdash; Amazon KCL now supports implementing record processors in any language by communicating with the daemon over [STDIN and STDOUT][multi-lang-protocol]. Python developers can directly use the [Amazon Kinesis Client Library for Python][kclpy] to write their data processing applications.

## Release 1.1 (June 30, 2014)
* **Checkpointing at a specific sequence number** &mdash; The IRecordProcessorCheckpointer interface now supports checkpointing at a sequence number specified by the record processor.
* **Set region** &mdash; KinesisClientLibConfiguration now supports setting the region name to indicate the location of the Amazon Kinesis service. The Amazon DynamoDB table and Amazon CloudWatch metrics associated with your application will also use this region setting.

[kinesis]: http://aws.amazon.com/kinesis
[kinesis-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=169
[kinesis-client-library-issues]: https://github.com/awslabs/amazon-kinesis-client/issues
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kinesis-guide]: http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html
[kinesis-guide-begin]: http://docs.aws.amazon.com/kinesis/latest/dev/before-you-begin.html
[kinesis-guide-create]: http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html
[kinesis-guide-applications]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html
[kinesis-guide-monitoring-with-kcl]: http://docs.aws.amazon.com//kinesis/latest/dev/monitoring-with-kcl.html
[kinesis-guide-kpl]: http://docs.aws.amazon.com//kinesis/latest/dev/developing-producers-with-kpl.html
[kinesis-guide-consumer-deaggregation]: http://docs.aws.amazon.com//kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kclpy]: https://github.com/awslabs/amazon-kinesis-client-python
[multi-lang-protocol]: https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java
