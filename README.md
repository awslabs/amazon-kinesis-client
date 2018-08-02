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

### Latest Release (2.0.0)
* Added support for Enhanced Fan Out.  
  Enhanced Fan Out provides for lower end to end latency, and increased number of consumers per stream. 
  * Records are now delivered via streaming, reducing end-to-end latency.
  * The Amazon Kinesis Client will automatically register a new consumer if required.  
    When registering a new consumer, the Kinesis Client will default to the application name unless configured otherwise.
  * New configuration options are available to configure Enhanced Fan Out. 
  * `SubscribeToShard` maintains long lived connections with Kinesis, which in the AWS Java SDK 2.0 is limited by default.  
    The `KinesisClientUtil` has been added to assist configuring the `maxConcurrency` of the `KinesisAsyncClient`.   
    __WARNING: The Amazon Kinesis Client may see significantly increased latency, unless the `KinesisAsyncClient` is configured to have a `maxConcurrency` high enough to allow all leases plus additional usages of the `KinesisAsyncClient`.__  
  
  | Name            | Default | Description                                                                                                         |
  |-----------------|---------|---------------------------------------------------------------------------------------------------------------------|
  | consumerArn     | Unset   | The ARN for an already created consumer.  If this is set, the Kinesis Client will not attempt to create a consumer. |
  | streamName      | Unset   | The name of the stream that a consumer should be create for if necessary                                            |
  | consumerName    | Unset   | The name of the consumer to create.  If this is not set the applicationName will be used instead.                   |
  | applicationName | Unset   | The name of the application.  This is used as the name of the consumer unless consumerName is set.                  |

* Modular Configuration of the Kinesis Client
  The Kinesis Client has migrated to a modular configuration system, and the `KinesisClientLibConfiguration` class has been removed.  
  Configuration has been split into 7 classes.  Default versions of the configuration can be created from the `ConfigsBuilder`.  
  Please [see the migration guide for more information][migration-guide].
  * `CheckpointConfig`
  * `CoordinatorConfig`
  * `LeaseManagementConfig`
  * `LifecycleConfig`
  * `MetricsConfig`
  * `ProcessorConfig`
  * `RetrievalConfig`

* Upgraded to AWS Java SDK 2.0  
  The Kinesis Client now uses the AWS Java SDK 2.0.  The dependency on AWS Java SDK 1.11 has been removed. 
  All configurations will only accept 2.0 clients.  
  * When configuring the `KinesisAsyncClient` the `KinesisClientUtil#createKinesisAsyncClient` can be used to configure the Kinesis Client 
  * __If you need support for AWS Java SDK 1.11 you will need to add a direct dependency.__  
    __When adding a dependency you must ensure that the 1.11 versions of Jackson dependencies are excluded__  
    [Please see the migration guide for more information][migration-guide]
    
* MultiLangDaemon is now a separate module  
  The MultiLangDaemon has been separated to its own Maven module and is no longer available in `amazon-kinesis-client`.  To include the MultiLangDaemon, add a dependency on `amazon-kinesis-client-multilang`.

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
