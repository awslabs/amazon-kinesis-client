# Configuring Credential Providers

[AwsCredentialProviders][aws-credentials-provider] are not a one-size-fits-all.
The AWS SDK provides a rich API to support various configurations for many different providers.
KCL multilang does not, and is not intended to, proxy the full breadth of the AWS SDK.
However, KCL now provides better extensibility to handle, and be enhanced to handle, additional configurations.
This document should help multilang customers configure a suitable `CredentialProvider` (or contribute changes to support a new use case!).

## Sample Provider Configuration

In a Properties file, an `AWSCredentialsProperty` configuration might look like:
```
AWSCredentialsProvider = STSAssumeRoleSessionCredentialsProvider|<arn>|<sessionName> 
```
This basic configuration creates an [STSAssumeRoleSessionCredentialsProvider][sts-assume-provider] with an ARN and session name.
While functional, this configuration is limited.
For example, this configuration cannot set a regional endpoint (e.g., VPC use case).

Leveraging nested properties, an `AWSCredentialsProperty` value might change to:
```
AWSCredentialsProvider = KclSTSAssumeRoleSessionCredentialsProvider|<arn>|<sessionName>\
    |endpointRegion=us-east-1|externalId=spartacus
```
N.B. Backslash (`\`) is for multi-line legibility and is not required.

## Nested Properties

KCL multilang supports "nested properties" on the `AWSCredentialsProvider` key in the properties file.
The [Backus-Naur form][bnf] of the value:
```
<property-value> ::= <provider-class> ["|" <required-param>]* ["|" <nested-property>]*
<provider-class> ::= <fully-qualified-provider-class> | <provider-class-name>
<required-param> ::= <string> # this depends on the provider
<nested-property> ::= <nested-key> "=" <nested-value>
<nested-key> ::= <lower-camel-case-key>
<nested-value ::= <string> # this depends on the nested key
```

In general, required parameters are passed directly to the class' constructor
(e.g., [STSAssumeRoleSessionCredentialsProvider(String, String)][sts-assume-provider-constructor]).

Nested properties are a custom mapping provided by KCL multilang, and do not exist in the AWS SDK.
See [NestedPropertyKey][nested-property-key] for the supported keys, and details on their expected values.

## Nested Property Processor

Nested keys are processed via [NestedPropertyProcessor][nested-property-processor].
Implementation is, obviously, dependent on the implementing class.
Adding a new nested key should be trivial.
A backwards-compatible addition might look like:
```
    default void acceptFoo(...) {
        // do nothing
    }
```

### KclSTSAssumeRoleSessionCredentialsProvider

KCL multilang includes a [custom nested property processor for `STSAssumeRole`][kcl-sts-provider].
Multilang configurations that use `STSAssumeRoleSessionCredentialsProvider` need only prefix `Kcl` to exercise this new provider:
```
AWSCredentialsProvider = KclSTSAssumeRoleSessionCredentialsProvider|<arn>|<sessionName>
```

[aws-credentials-provider]: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html
[bnf]: https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form
[kcl-sts-provider]: /amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/multilang/auth/KclSTSAssumeRoleSessionCredentialsProvider.java
[nested-property-key]: /amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/multilang/NestedPropertyKey.java
[nested-property-processor]: /amazon-kinesis-client-multilang/src/main/java/software/amazon/kinesis/multilang/NestedPropertyProcessor.java
[sts-assume-provider]: https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html
[sts-assume-provider-constructor]: https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html#STSAssumeRoleSessionCredentialsProvider-java.lang.String-java.lang.String-
