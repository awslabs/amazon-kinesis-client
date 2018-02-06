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

## Release Notes

### Latest Release (1.9.0)
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
