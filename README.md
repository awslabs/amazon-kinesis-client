# Amazon Kinesis Client Library for Java
[![Build Status](https://travis-ci.org/awslabs/amazon-kinesis-client.svg?branch=master)](https://travis-ci.org/awslabs/amazon-kinesis-client) ![BuildStatus](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiaWo4bDYyUkpWaG9ZTy9zeFVoaVlWbEwxazdicDJLcmZwUUpFWVVBM0ZueEJSeFIzNkhURzdVbUd6WUZHcGNxa3BEUzNrL0I5Nzc4NE9rbXhvdEpNdlFRPSIsIml2UGFyYW1ldGVyU3BlYyI6IlZDaVZJSTM1QW95bFRTQnYiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=v1.x)

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

## Latest Release (1.12.0 October 17, 2019)
[Milestone#41](https://github.com/awslabs/amazon-kinesis-client/milestone/41)
* Adding logging around shard end codepaths
  * [PR #585](https://github.com/awslabs/amazon-kinesis-client/pull/585)
* Updating checkpointing failure message to refer to javadocs
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

### Release 1.11.1 (August 9, 2019)
[Milestone#34](https://github.com/awslabs/amazon-kinesis-client/milestone/34)
* Updated the version of the AWS Java SDK to 1.11.603.
  * [PR #587](https://github.com/awslabs/amazon-kinesis-client/pull/587)
* Added logging to `KinesisDataFetcher` when reaching the end of a shard due to a null next iterator.
  * [PR #585](https://github.com/awslabs/amazon-kinesis-client/pull/585)
  
### Release 1.11.0 (August 7, 2019)
[Milestone#33](https://github.com/awslabs/amazon-kinesis-client/milestone/33)
* Improved exception handling and logging in `KinesisClientLibLeaseCoordinator` to avoid `NullPointerExceptions` when no leases are found.
  * [PR #558](https://github.com/awslabs/amazon-kinesis-client/pull/558)
* Introducing optional new periodic shard sync strategy to perform shard discovery and lease cleanup on a single worker.
  * [PR #579](https://github.com/awslabs/amazon-kinesis-client/pull/579)

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
