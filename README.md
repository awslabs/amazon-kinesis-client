# Amazon Kinesis Client Library for Java
[![Build Status](https://travis-ci.org/awslabs/amazon-kinesis-client.svg?branch=master)](https://travis-ci.org/awslabs/amazon-kinesis-client)

The **Amazon Kinesis Client Library for Java** (Amazon KCL) enables Java developers to easily consume and process data from [Amazon Kinesis][kinesis].

* [Kinesis Product Page][kinesis]
* [Forum][kinesis-forum]
* [Javadoc][kcl-javadoc]
* [FAQ](docs/FAQ.md)
* [KCL Documentation](docs/) (folder)
* [Issues][kinesis-client-library-issues]

### Recommended Upgrade for All Users of the 1.x Amazon Kinesis Client
:warning: We recommend customers to migrate to 1.14.1 or newer to avoid [known bugs](https://github.com/awslabs/amazon-kinesis-client/issues/778) in 1.14.0 version

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

After you've downloaded the code from GitHub, you can build it using Maven. To disable GPG signing in the build, use
this command: `mvn clean install -Dgpg.skip=true`. 
Note: This command does not run integration tests.

## Running Integration Tests

Note that running integration tests creates AWS resources.
Integration tests require valid AWS credentials.
This will look for a default AWS profile specified in your local `.aws/credentials`.
To run all integration tests: `mvn verify -DskipITs=false`.
To run one integration tests: `mvn -Dit.test=*IntegrationTest -DskipITs=false verify`
Optionally, you can provide the name of an IAM user/role to run tests with as a string using this command: `mvn verify -DskipITs=false -DawsProfile="<PROFILE_NAME>"`.

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
      <version>2.5.7</version>
  </dependency>
  ```

### Version 1.x
[Version 1.x tracking branch](https://github.com/awslabs/amazon-kinesis-client/tree/v1.x)
  ``` xml
  <dependency>
      <groupId>com.amazonaws</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>1.14.1</version>
  </dependency>
  ```

## Release Notes

| KCL Version | Changelog |
| --- | --- |
| 2.x | [master/CHANGELOG.md](CHANGELOG.md) |
| 1.x | [v1.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v1.x/CHANGELOG.md) |

[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kcl-javadoc]: https://javadoc.io/doc/software.amazon.kinesis/amazon-kinesis-client/
[kinesis]: http://aws.amazon.com/kinesis
[kinesis-client-library-issues]: https://github.com/awslabs/amazon-kinesis-client/issues
[kinesis-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=169
[kinesis-guide]: http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html
[kinesis-guide-begin]: http://docs.aws.amazon.com/kinesis/latest/dev/before-you-begin.html
[kinesis-guide-create]: http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html
[kinesis-guide-applications]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html
[kinesis-guide-monitoring-with-kcl]: http://docs.aws.amazon.com//kinesis/latest/dev/monitoring-with-kcl.html
[kinesis-guide-kpl]: http://docs.aws.amazon.com//kinesis/latest/dev/developing-producers-with-kpl.html
[kinesis-guide-consumer-deaggregation]: http://docs.aws.amazon.com//kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kclpy]: https://github.com/awslabs/amazon-kinesis-client-python
[multi-lang-protocol]: /amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/multilang/package-info.java
[migration-guide]: https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html
