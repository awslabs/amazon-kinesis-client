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
      <version>2.0.1</version>
  </dependency>
  ```

### Version 1.x
  ``` xml
  <dependency>
      <groupId>com.amazonaws</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>1.9.1</version>
  </dependency>
  ```


## Release Notes

### Latest Release (2.0.1)
* Mark certain internal components with `@KinesisClientInternalApi` attribute.  
  Components marked as internal may be deprecated at a faster rate than public components.  
  * [PR #358](https://github.com/awslabs/amazon-kinesis-client/pull/358)
* Fixed an issue where `ResourceNotFoundException` on subscription to a shard was not triggering end of shard handling.  
  If a lease table contains a shard that is no longer present in the stream attempt to subscribe to that shard will trigger a `ResourceNotFoundException`. These exception are treated the same as reaching the end of a shard.
  * [PR #359](https://github.com/awslabs/amazon-kinesis-client/pull/359)
* Fixed an issue where the KCL would not Use the configured DynamoDB IOPs when creating the lease table.  
  * [PR #360](https://github.com/awslabs/amazon-kinesis-client/pull/360)
* Make the maximum number of Scheduler initialization attempts configurable.  
  The maximum number of `Scheduler` initialization attempts can be configured via `CoordinatorConfig#maxInitializationAttempts`.
  * [PR #363](https://github.com/awslabs/amazon-kinesis-client/pull/363)
  * [PR #368](https://github.com/awslabs/amazon-kinesis-client/pull/368)
* Fixed an issue where it was possible to get a duplicate record when resubscribing to a shard.  
  Subscribe to shard requires periodic resubscribing, and uses a new concept of a continuation sequence number.  If the continuation sequence number was equal to the last record that record would be processed a second time.  Resubscribing now uses `AFTER_SEQUENCE_NUMBER` to ensure that only later records are returned.  
  * [PR #371](https://github.com/awslabs/amazon-kinesis-client/pull/371)
* Upgraded to AWS SDK 2.0.1  
  * [PR #372](https://github.com/awslabs/amazon-kinesis-client/pull/372)
* Fixed an issue where time based restart of the subscription wasn't resetting the `lastRequestTime`.  
  If a subscription hasn't delivered any data for more than 30 seconds it will be canceled and restarted.  This detection is based of the `lastRequestTime` which wasn't getting reset after the restart was triggered.
  * [PR #373](https://github.com/awslabs/amazon-kinesis-client/pull/373)
* Fixed an issue where requesting on the subscription from the `FanOutRecordsPublisher` could trigger an unexpected failure.  
  Due to a race condition the underlying flow in the subscription could be set to something else.  The method is now synchronized, and verifies that the subscriber it was created with is still the subscriber in affect.  
  This issue generally would only appear when multiple errors were occurring while connecting to Kinesis.
  * [PR #374](https://github.com/awslabs/amazon-kinesis-client/pull/374)
* Fixed an issue where the number of requested items could exceed the capacity of the RxJava queue.  
  There was an off by one issue when determining whether to make a request to the SDK subscription.  This changes the calculation to represent the capacity as a queue.
  * [PR #375](https://github.com/awslabs/amazon-kinesis-client/pull/375)

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
[multi-lang-protocol]: https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java
[changelog-md]: https://github.com/awslabs/amazon-kinesis-client/blob/master/CHANGELOG.md
[migration-guide]: https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html
