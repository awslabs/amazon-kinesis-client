# Amazon Kinesis Client Library for Java
[![Build Status](https://travis-ci.org/awslabs/amazon-kinesis-client.svg?branch=master)](https://travis-ci.org/awslabs/amazon-kinesis-client) ![BuildStatus](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiaWo4bDYyUkpWaG9ZTy9zeFVoaVlWbEwxazdicDJLcmZwUUpFWVVBM0ZueEJSeFIzNkhURzdVbUd6WUZHcGNxa3BEUzNrL0I5Nzc4NE9rbXhvdEpNdlFRPSIsIml2UGFyYW1ldGVyU3BlYyI6IlZDaVZJSTM1QW95bFRTQnYiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)

The **Amazon Kinesis Client Library for Java** (Amazon KCL) enables Java developers to easily consume and process data from [Amazon Kinesis][kinesis].

* [Kinesis Product Page][kinesis]
* [Forum][kinesis-forum]
* [Issues][kinesis-client-library-issues]

### Recommended Upgrade for All Users of the 2.x Amazon Kinesis Client
**:warning: It's highly recommended for users of version 2.0 of the Amazon Kinesis Client to upgrade to version 2.0.3 or later. A [bug has been](https://github.com/awslabs/amazon-kinesis-client/issues/391) identified in versions prior to 2.0.3 that could cause records to be delivered to the wrong record processor.**  

**:information_source: Amazon Kinesis Client versions 1.x are not impacted.**  

Please open an issue if you have any questions.

## Features

* Provides an easy-to-use programming model for processing data using Amazon Kinesis
* Helps with scale-out and fault-tolerant processing

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
1. **Sign up for Amazon Kinesis** &mdash; Go to the Amazon Kinesis console to sign up for the service and create an Amazon Kinesis stream. For more information, see [Create an Amazon Kinesis Stream][kinesis-guide-create] in the Amazon Kinesis Developer Guide.
1. **Minimum requirements** &mdash; To use the Amazon Kinesis Client Library, you'll need **Java 1.8+**. For more information about Amazon Kinesis Client Library requirements, see [Before You Begin][kinesis-guide-begin] in the Amazon Kinesis Developer Guide.
1. **Using the Amazon Kinesis Client Library** &mdash; The best way to get familiar with the Amazon Kinesis Client Library is to read [Developing Record Consumer Applications][kinesis-guide-applications] in the Amazon Kinesis Developer Guide.

## Building from Source

After you've downloaded the code from GitHub, you can build it using Maven. To disable GPG signing in the build, use this command: `mvn clean install -Dgpg.skip=true`

## Integration with the Kinesis Producer Library
For producer-side developers using the **[Kinesis Producer Library (KPL)][kinesis-guide-kpl]**, the KCL integrates without additional effort. When the KCL retrieves an aggregated Amazon Kinesis record consisting of multiple KPL user records, it will automatically invoke the KPL to extract the individual user records before returning them to the user.

## Amazon KCL support for other languages
To make it easier for developers to write record processors in other languages, we have implemented a Java based daemon, called MultiLangDaemon that does all the heavy lifting. Our approach has the daemon spawn a sub-process, which in turn runs the record processor, which can be written in any language. The MultiLangDaemon process and the record processor sub-process communicate with each other over [STDIN and STDOUT using a defined protocol][multi-lang-protocol]. There will be a one to one correspondence amongst record processors, child processes, and shards. For Python developers specifically, we have abstracted these implementation details away and [expose an interface][kclpy] that enables you to focus on writing record processing logic in Python. This approach enables KCL to be language agnostic, while providing identical features and similar parallel processing model across all languages.

## Using the KCL
The recommended way to use the KCL for Java is to consume it from Maven.

### Version 2.x
  ``` xml
  <dependency>
      <groupId>software.amazon.kinesis</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>2.2.4</version>
  </dependency>
  ```

### Version 1.x
[Version 1.x tracking branch](https://github.com/awslabs/amazon-kinesis-client/tree/v1.x)
  ``` xml
  <dependency>
      <groupId>com.amazonaws</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>1.11.2</version>
  </dependency>
  ```

## Release Notes

### Latest Release (2.2.6 - November 7, 2019)
[Milestone#43](https://github.com/awslabs/amazon-kinesis-client/milestone/43)
* Updating the SDK version to 2.9.25.
  * [PR#638](https://github.com/awslabs/amazon-kinesis-client/pull/638)
* Clearing the local cache on a subscription termination, to avoid noisy logs on new subscriptions.
  * [PR#642](https://github.com/awslabs/amazon-kinesis-client/pull/642)
* Updating the SDK version to 2.10.0 in order to fix the premature H2 stream close issue.
  * [PR#649](https://github.com/awslabs/amazon-kinesis-client/pull/649)
  * NOTE: SDK has a known connection teardown issue when multiple H2 streams are used within a connection. This might result in shard consumers sticking to a stale service host and not progressing. If your shard consumer gets stuck, use the following configuration as a workaround. This configuration might result in up to 5X increase in total connections.
  ```
  KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder()
                                                       .region(region)
                                                       .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(Integer.MAX_VALUE).maxHttp2Streams(1))
                                                       .build()
  ```

### Related Prior Release (2.2.5 - October 23, 2019)
[Milestone#40](https://github.com/awslabs/amazon-kinesis-client/milestone/40)
* Updating Sonatype to dedicated AWS endpoint.
  * [PR#619](https://github.com/awslabs/amazon-kinesis-client/pull/619)
* Introducing a validation step to verify if ShardEnd is reached, to prevent shard consumer stuck scenarios in the event of malformed response from service.
  * [PR#624](https://github.com/awslabs/amazon-kinesis-client/pull/624)

### Related Prior Release (2.2.4 - September 23, 2019)
[Milestone#39](https://github.com/awslabs/amazon-kinesis-client/milestone/39)
* Making FanoutRecordsPublisher test cases resilient to delayed thread operations
  * [PR#612](https://github.com/awslabs/amazon-kinesis-client/pull/612)
* Drain delivery queue in the FanoutRecordsPublisher to make slow consumers consume events at their pace
  * [PR#607](https://github.com/awslabs/amazon-kinesis-client/pull/607)
* Fix to prevent the onNext event going to stale subscription when restart happens in PrefetchRecordsPublisher
  * [PR#606](https://github.com/awslabs/amazon-kinesis-client/pull/606)

### Related Prior Release (2.2.3 - September 04, 2019)
[Milestone#38](https://github.com/awslabs/amazon-kinesis-client/milestone/38)
* Fix to prevent data loss and stuck shards in the event of failed records delivery in Polling readers
  * [PR#603](https://github.com/awslabs/amazon-kinesis-client/pull/603)

### Related Prior Release (2.2.2 - August 19, 2019)
[Milestone#36](https://github.com/awslabs/amazon-kinesis-client/milestone/36)
* Fix to prevent invalid ShardConsumer state transitions due to rejected executor service executions.
  * [PR#560](https://github.com/awslabs/amazon-kinesis-client/pull/560)
* Fixing a bug in which initial subscription failure caused a shard consumer to get stuck.
  * [PR#562](https://github.com/awslabs/amazon-kinesis-client/pull/562)
* Making CW publish failures visible by executing the async publish calls in a blocking manner and logging on exception.
  * [PR#584](https://github.com/awslabs/amazon-kinesis-client/pull/584)
* Update shard end checkpoint failure messaging.
  * [PR#591](https://github.com/awslabs/amazon-kinesis-client/pull/591)
* A fix for resiliency and durability issues that occur in the reduced thread mode - Nonblocking approach.
  * [PR#573](https://github.com/awslabs/amazon-kinesis-client/pull/573)
* Preventing duplicate delivery due to unacknowledged event, while completing the subscription.
  * [PR#596](https://github.com/awslabs/amazon-kinesis-client/pull/596)


### For remaining release notes check **[CHANGELOG.md][changelog-md]**.

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
[multi-lang-protocol]: https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/multilang/package-info.java
[changelog-md]: https://github.com/awslabs/amazon-kinesis-client/blob/master/CHANGELOG.md
[migration-guide]: https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html
