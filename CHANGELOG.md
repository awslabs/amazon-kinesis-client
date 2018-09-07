# Changelog

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
