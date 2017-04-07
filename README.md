# Amazon Kinesis Client Library for Java [![Build Status](https://travis-ci.org/awslabs/amazon-kinesis-client.svg?branch=master)](https://travis-ci.org/awslabs/amazon-kinesis-client)

The **Amazon Kinesis Client Library for Java** (Amazon KCL) enables Java developers to easily consume and process data from [Amazon Kinesis][kinesis].

* [Kinesis Product Page][kinesis]
* [Forum][kinesis-forum]
* [Issues][kinesis-client-library-issues]

## Features

* Provides an easy-to-use programming model for processing data using Amazon Kinesis
* Helps with scale-out and fault-tolerant processing

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
1. **Sign up for Amazon Kinesis** &mdash; Go to the Amazon Kinesis console to sign up for the service and create an Amazon Kinesis stream. For more information, see [Create an Amazon Kinesis Stream][kinesis-guide-create] in the Amazon Kinesis Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon Kinesis Client Library, you'll need **Java 1.7+**. For more information about Amazon Kinesis Client Library requirements, see [Before You Begin][kinesis-guide-begin] in the Amazon Kinesis Developer Guide.
1. **Using the Amazon Kinesis Client Library** &mdash; The best way to get familiar with the Amazon Kinesis Client Library is to read [Developing Record Consumer Applications][kinesis-guide-applications] in the Amazon Kinesis Developer Guide.

## Building from Source

After you've downloaded the code from GitHub, you can build it using Maven. To disable GPG signing in the build, use this command: `mvn clean install -Dgpg.skip=true`

## Integration with the Kinesis Producer Library
For producer-side developers using the **[Kinesis Producer Library (KPL)][kinesis-guide-kpl]**, the KCL integrates without additional effort.  When the KCL retrieves an aggregated Amazon Kinesis record consisting of multiple KPL user records, it will automatically invoke the KPL to extract the individual user records before returning them to the user.

## Amazon KCL support for other languages
To make it easier for developers to write record processors in other languages, we have implemented a Java based daemon, called MultiLangDaemon that does all the heavy lifting. Our approach has the daemon spawn a sub-process, which in turn runs the record processor, which can be written in any language. The MultiLangDaemon process and the record processor sub-process communicate with each other over [STDIN and STDOUT using a defined protocol][multi-lang-protocol]. There will be a one to one correspondence amongst record processors, child processes, and shards. For Python developers specifically, we have abstracted these implementation details away and [expose an interface][kclpy] that enables you to focus on writing record processing logic in Python. This approach enables KCL to be language agnostic, while providing identical features and similar parallel processing model across all languages.

## Release Notes
### Release 1.7.5 (April 7, 2017)
* Correctly handle throttling for DescribeStream, and save accumulated progress from individual calls.
  * [PR #152](https://github.com/awslabs/amazon-kinesis-client/pull/152)
* Upgrade to version 1.11.115 of the AWS Java SDK
  * [PR #155](https://github.com/awslabs/amazon-kinesis-client/pull/155)
  
### Release 1.7.4 (February 27, 2017)
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

### Release 1.7.3 (January 9, 2017)
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

### Release 1.7.2 (November 7, 2016)
* MultiLangDaemon Feature Updates
  The MultiLangDaemon has been upgraded to use the v2 interfaces, which allows access to enhanced checkpointing, and more information during record processor initialization. The MultiLangDaemon clients must be updated before they can take advantage of these new features.
  
### Release 1.7.1 (November 3, 2016)
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

### Release 1.7.0 (August 22, 2016)
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

### Release 1.6.5 (July 25, 2016)
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

### Release 1.6.4 (July 6, 2016)
* Upgrade to AWS SDK for Java 1.11.14
  * [Issue #74](https://github.com/awslabs/amazon-kinesis-client/issues/74)
  * [Issue #73](https://github.com/awslabs/amazon-kinesis-client/issues/73)
* **Maven Artifact Signing Change** 
  * Artifacts are now signed by the identity `Amazon Kinesis Tools <amazon-kinesis-tools@amazon.com>`

### Release 1.6.3 (May 12, 2016)
* Fix format exception caused by DEBUG log in LeaseTaker [Issue # 68](https://github.com/awslabs/amazon-kinesis-client/issues/68)

### Release 1.6.2 (March 23, 2016)
* Support for specifying max leases per worker and max leases to steal at a time.
* Support for specifying initial DynamoDB table read and write capacity.
* Support for parallel lease renewal.
* Support for graceful worker shutdown.
* Change DefaultCWMetricsPublisher log level to debug. [PR # 49](https://github.com/awslabs/amazon-kinesis-client/pull/49)
* Avoid NPE in MLD record processor shutdown if record processor was not initialized. [Issue # 29](https://github.com/awslabs/amazon-kinesis-client/issues/29)

### Release 1.6.1 (September 23, 2015)
* Expose [approximateArrivalTimestamp](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) for Records in processRecords API call.

### Release 1.6.0 (July 31, 2015)
* Restores compatibility with [dynamodb-streams-kinesis-adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter) (which was broken in 1.4.0).

### Release 1.5.1 (July 20, 2015)
* KCL maven artifact 1.5.0 does not work with JDK 7. This release addresses this issue.

### Release 1.5.0 (July 9, 2015)
* **[Metrics Enhancements][kinesis-guide-monitoring-with-kcl]**
	* Support metrics level and dimension configurations to control CloudWatch metrics emitted by the KCL.
	* Add new metrics that track time spent in record processor methods.
	* Disable WorkerIdentifier dimension by default.
* **Exception Reporting** &mdash; Do not silently ignore exceptions in ShardConsumer.
* **AWS SDK Component Dependencies** &mdash; Depend only on AWS SDK components that are used.

### Release 1.4.0 (June 2, 2015)
* Integration with the **[Kinesis Producer Library (KPL)][kinesis-guide-kpl]**
	* Automatically de-aggregate records put into the Kinesis stream using the KPL.
	* Support checkpointing at the individual user record level when multiple user records are aggregated into one Kinesis record using the KPL.

 See [Consumer De-aggregation with the KCL][kinesis-guide-consumer-deaggregation] for details.

### Release 1.3.0 (May 22, 2015)
* A new metric called "MillisBehindLatest", which tracks how far consumers are from real time, is now uploaded to CloudWatch.

### Release 1.2.1 (January 26, 2015)
* **MultiLangDaemon** &mdash; Changes to the MultiLangDaemon to make it easier to provide a custom worker.

### Release 1.2 (October 21, 2014)
* **Multi-Language Support** &mdash; Amazon KCL now supports implementing record processors in any language by communicating with the daemon over [STDIN and STDOUT][multi-lang-protocol]. Python developers can directly use the [Amazon Kinesis Client Library for Python][kclpy] to write their data processing applications.

### Release 1.1 (June 30, 2014)
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

