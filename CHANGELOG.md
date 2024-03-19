# Changelog

For **1.x** release notes, please see [v1.x/CHANGELOG.md](https://github.com/awslabs/amazon-kinesis-client/blob/v1.x/CHANGELOG.md)

---
### Release 2.5.7 (2024-03-19)
* [#1275](https://github.com/awslabs/amazon-kinesis-client/pull/1275) Update PollingConfig maxRecords method to return PollingConfig 
* [#1236](https://github.com/awslabs/amazon-kinesis-client/pull/1236) Upgrade commons-io:commons-io from 2.11.0 to 2.15.1 
* [#1189](https://github.com/awslabs/amazon-kinesis-client/pull/1189) Upgrade org.apache.maven.plugins:maven-resources-plugin from 3.3.0 to 3.3.1 
* [#1139](https://github.com/awslabs/amazon-kinesis-client/pull/1139) Upgrade maven-surefire-plugin from 2.22.2 to 3.1.2 
* [#1138](https://github.com/awslabs/amazon-kinesis-client/pull/1138) Upgrade maven-failsafe-plugin from 2.22.2 to 3.1.2 
* [#1125](https://github.com/awslabs/amazon-kinesis-client/pull/1125) Upgrade maven-gpg-plugin from 3.0.1 to 3.1.0 

### Release 2.5.6 (2024-03-08)
* [#1271](https://github.com/awslabs/amazon-kinesis-client/pull/1271) Adding snapshot for 2.5.6-SNAPSHOT 
* [#1268](https://github.com/awslabs/amazon-kinesis-client/pull/1268) Upgrade ch.qos.logback:logback-classic dependency from 1.3.12 to 1.3.14
* [#1268](https://github.com/awslabs/amazon-kinesis-client/pull/1268) Upgrade awssdk.version from 2.20.43 to 2.25.3
* [#1268](https://github.com/awslabs/amazon-kinesis-client/pull/1268) Upgrade aws-java-sdk.version from 1.12.405 to 1.12.668
* [#1268](https://github.com/awslabs/amazon-kinesis-client/pull/1268) Upgrade gsr.version from 1.1.17 to 1.1.19

### Release 2.5.5 (2024-02-22)
* [#1257](https://github.com/awslabs/amazon-kinesis-client/pull/1257) Prevent improper error logging during worker shutdown 
* [#1260](https://github.com/awslabs/amazon-kinesis-client/pull/1260) Add Deletion protection config 
* [#1258](https://github.com/awslabs/amazon-kinesis-client/pull/1258) Fix issue in configuring metricsEnabledDimensions 
* [#1259](https://github.com/awslabs/amazon-kinesis-client/pull/1259) Add snapshot to version 

### Release 2.5.4 (December 12, 2023)
* [#1232](https://github.com/awslabs/amazon-kinesis-client/pull/1232) Upgrade ch.qos.logback:logback-classic dependency from 1.3.0 to 1.3.12 in /amazon-kinesis-client
* [#1233](https://github.com/awslabs/amazon-kinesis-client/pull/1233) Upgrade ch.qos.logback:logback-classic dependency from 1.3.0 to 1.3.12 in /amazon-kinesis-client-multilang
* [#1230](https://github.com/awslabs/amazon-kinesis-client/pull/1230) Bug fix which now allows MultiLangDaemon to configure idleTimeBetweenReadsInMillis
* [#1229](https://github.com/awslabs/amazon-kinesis-client/pull/1229) Added link to `javadoc.io`-hosted Javadoc in the README
* [#1218](https://github.com/awslabs/amazon-kinesis-client/pull/1218) Added doc for leases and the lease lifecycle to help explain lease lifecycle logic.
* [#1226](https://github.com/awslabs/amazon-kinesis-client/pull/1226) Upgraded KCL from 2.5.3 to 2.5.4-SNAPSHOT

### Release 2.5.3 (November 8, 2023)
* [#1219](https://github.com/awslabs/amazon-kinesis-client/pull/1219) Provided streamArn in getRecords request
* [#1216](https://github.com/awslabs/amazon-kinesis-client/pull/1216) Updated AWS Glue Schema Registry from version 1.1.14 to 1.1.17.
* [#1205](https://github.com/awslabs/amazon-kinesis-client/pull/1205) Updated the FAQ with impact of changing default checkpoint.
* [#1203](https://github.com/awslabs/amazon-kinesis-client/pull/1203) Added links from README.md to FAQ and doc folder.
* [#1202](https://github.com/awslabs/amazon-kinesis-client/pull/1202) Introduced a FAQ for Kinesis Client Library
* [#1200](https://github.com/awslabs/amazon-kinesis-client/pull/1200) Added test case for StreamIdentifier serialization.

### Release 2.5.2 (August 7, 2023)
* [#1184](https://github.com/awslabs/amazon-kinesis-client/pull/1184) [#367] Enhanced multi-lang `AWSCredentialsProvider=...` decoder and c…
* [#1186](https://github.com/awslabs/amazon-kinesis-client/pull/1186) Provided documentation for multilang's new NestedPropertyKey enhancement.
* [#1181](https://github.com/awslabs/amazon-kinesis-client/pull/1181) CVE-2023-2976: Upgrade Google Guava dependency version from `32.0.0-jre` to `32.1.1-jre`
* [#1159](https://github.com/awslabs/amazon-kinesis-client/pull/1159) Bug fix in lease refresher integration test with occasional failures
* [#1157](https://github.com/awslabs/amazon-kinesis-client/pull/1157) Fix NPE on graceful shutdown before DDB `LeaseCoordinator` starts.
* [#1152](https://github.com/awslabs/amazon-kinesis-client/pull/1152) Adding resharding integration tests and changing ITs to not run by default
* [#1162](https://github.com/awslabs/amazon-kinesis-client/pull/1162) Only deleting resource created by ITs
* [#1158](https://github.com/awslabs/amazon-kinesis-client/pull/1158) Checkstyle: tightened `LineLength` restriction from 170 to 150.
* [#1151](https://github.com/awslabs/amazon-kinesis-client/pull/1151) Modified `dependabot.yml` to set the correct `v[1|2].x` label.
* [#1164](https://github.com/awslabs/amazon-kinesis-client/pull/1164) Upgraded KCL Version from 2.5.1 to 2.5.2-SNAPSHOT

### Release 2.5.1 (June 27, 2023)
* [#1143](https://github.com/awslabs/amazon-kinesis-client/pull/1143) Upgrade MultiLangDaemon to support StreamARN
* [#1145](https://github.com/awslabs/amazon-kinesis-client/pull/1145) Introduced GitHub actions to trigger Maven builds during merge/pull requests
* [#1136](https://github.com/awslabs/amazon-kinesis-client/pull/1136) Added testing architecture and KCL 2.x basic polling/streaming tests
* [#1153](https://github.com/awslabs/amazon-kinesis-client/pull/1153) Checkstyle: added `UnusedImports` check.
* [#1150](https://github.com/awslabs/amazon-kinesis-client/pull/1150) Enabled Checkstyle validation of test resources.
* [#1149](https://github.com/awslabs/amazon-kinesis-client/pull/1149) Bound Checkstyle to `validate` goal for automated enforcement.
* [#1148](https://github.com/awslabs/amazon-kinesis-client/pull/1148) Code cleanup to faciliate Checkstyle enforcement.
* [#1142](https://github.com/awslabs/amazon-kinesis-client/pull/1142) Upgrade Google Guava dependency version from 31.1-jre to 32.0.0-jre
* [#1115](https://github.com/awslabs/amazon-kinesis-client/pull/1115) Update KCL version from 2.5.0 to 2.5.1-SNAPSHOT

### Release 2.5.0 (May 19, 2023)
* **[#1109](https://github.com/awslabs/amazon-kinesis-client/pull/1109) Add support for stream ARNs**
* **[#1065](https://github.com/awslabs/amazon-kinesis-client/pull/1065) Allow tags to be added when lease table is created**
* [#1094](https://github.com/awslabs/amazon-kinesis-client/pull/1094) Code cleanup to introduce better testing
* [#1088](https://github.com/awslabs/amazon-kinesis-client/pull/1088) Minimize race in PSSM to optimize shard sync calls
* [#1086](https://github.com/awslabs/amazon-kinesis-client/pull/1086) Add additional SingleStreamTracker constructor with stream position parameter
* [#1084](https://github.com/awslabs/amazon-kinesis-client/pull/1084) More consistent testing behavior with restartAfterRequestTimerExpires
* [#1066](https://github.com/awslabs/amazon-kinesis-client/pull/1066) More consistent testing behavior with HashRangesAreAlwaysComplete
* [#1072](https://github.com/awslabs/amazon-kinesis-client/pull/1072) Upgrade nexus-staging-maven-plugin from 1.6.8 to 1.6.13
* [#1073](https://github.com/awslabs/amazon-kinesis-client/pull/1073) Upgrade slf4j-api from 2.0.6 to 2.0.7
* [#1090](https://github.com/awslabs/amazon-kinesis-client/pull/1090) Upgrade awssdk.version from 2.20.8 to 2.20.43
* [#1071](https://github.com/awslabs/amazon-kinesis-client/pull/1071) Upgrade maven-compiler-plugin from 3.8.1 to 3.11.0

### Release 2.4.8 (March 21, 2023)
* [#1080](https://github.com/awslabs/amazon-kinesis-client/pull/1080) Added metric in `ShutdownTask` for scenario when parent leases are missing.
* [#1077](https://github.com/awslabs/amazon-kinesis-client/pull/1077) Reverted changes to pom property
* [#1069](https://github.com/awslabs/amazon-kinesis-client/pull/1069) Fixed flaky InitializationWaitsWhenLeaseTableIsEmpty test


### Release 2.4.7 (March 17, 2023)
* **NOTE: Due to an issue during the release process, the 2.4.7 published artifacts are incomplete and non-viable. Please use 2.4.8 or later.**
* [#1063](https://github.com/awslabs/amazon-kinesis-client/pull/1063) Allow leader to learn new leases upon re-election to avoid unnecessary shardSyncs
* [#1060](https://github.com/awslabs/amazon-kinesis-client/pull/1060) Add new metric to be emitted on lease creation
* [#1057](https://github.com/awslabs/amazon-kinesis-client/pull/1057) Added more logging in `Scheduler` w.r.t. `StreamConfig`s.
* [#1059](https://github.com/awslabs/amazon-kinesis-client/pull/1059) DRY: simplification of `HierarchicalShardSyncerTest`.
* [#1062](https://github.com/awslabs/amazon-kinesis-client/pull/1062) Fixed retry storm in `PrefetchRecordsPublisher`.
* [#1061](https://github.com/awslabs/amazon-kinesis-client/pull/1061) Fixed NPE in `LeaseCleanupManager`.
* [#1056](https://github.com/awslabs/amazon-kinesis-client/pull/1056) Clean up in-memory state of deleted kinesis stream in MultiStreamMode
* [#1058](https://github.com/awslabs/amazon-kinesis-client/pull/1058) Documentation: added `<pre>` tags so fixed-format diagrams aren't garbled.
* [#1053](https://github.com/awslabs/amazon-kinesis-client/pull/1053) Exposed convenience method of `ExtendedSequenceNumber#isSentinelCheckpoint()`
* [#1043](https://github.com/awslabs/amazon-kinesis-client/pull/1043) Removed a `.swp` file, and updated `.gitignore`.
* [#1047](https://github.com/awslabs/amazon-kinesis-client/pull/1047) Upgrade awssdk.version from 2.19.31 to 2.20.8
* [#1046](https://github.com/awslabs/amazon-kinesis-client/pull/1046) Upgrade maven-javadoc-plugin from 3.3.1 to 3.5.0
* [#1038](https://github.com/awslabs/amazon-kinesis-client/pull/1038) Upgrade gsr.version from 1.1.13 to 1.1.14
* [#1037](https://github.com/awslabs/amazon-kinesis-client/pull/1037) Upgrade aws-java-sdk.version from 1.12.370 to 1.12.405

### Release 2.4.6 (February 21, 2023)
* [#1041](https://github.com/awslabs/amazon-kinesis-client/pull/1041) Minor optimizations (e.g., calculate-once, put instead of get+put)
* [#1035](https://github.com/awslabs/amazon-kinesis-client/pull/1035) Release Note updates to avoid duplication and bitrot (e.g., 1.x release
* [#935](https://github.com/awslabs/amazon-kinesis-client/pull/935) Pass isAtShardEnd correctly to processRecords call
* [#1040](https://github.com/awslabs/amazon-kinesis-client/pull/1040) Increased logging verbosity around lease management
* [#1024](https://github.com/awslabs/amazon-kinesis-client/pull/1024) Added logging w.r.t. StreamConfig handling.
* [#1034](https://github.com/awslabs/amazon-kinesis-client/pull/1034) Optimization: 9~15% improvement in KinesisDataFetcher wall-time
* [#1045](https://github.com/awslabs/amazon-kinesis-client/pull/1045) Fixed duplication of project version in children pom.xml
* [#956](https://github.com/awslabs/amazon-kinesis-client/pull/956) Fixed warning message typos
* [#795](https://github.com/awslabs/amazon-kinesis-client/pull/795) Fixed log message spacing
* [#740](https://github.com/awslabs/amazon-kinesis-client/pull/740) Fixed typo in Comment
* [#1028](https://github.com/awslabs/amazon-kinesis-client/pull/1028) Refactored MultiStreamTracker to provide and enhance OOP for both
* [#1027](https://github.com/awslabs/amazon-kinesis-client/pull/1027) Removed CHECKSTYLE:OFF toggles which can invite/obscure sub-par code.
* [#1032](https://github.com/awslabs/amazon-kinesis-client/pull/1032) Upgrade rxjava from 3.1.5 to 3.1.6
* [#1030](https://github.com/awslabs/amazon-kinesis-client/pull/1030) Upgrade awssdk.version from 2.19.2 to 2.19.31
* [#1029](https://github.com/awslabs/amazon-kinesis-client/pull/1029) Upgrade slf4j-api from 2.0.0 to 2.0.6
* [#1015](https://github.com/awslabs/amazon-kinesis-client/pull/1015) Upgrade protobuf-java from 3.21.5 to 3.21.12

### Release 2.4.5 (January 04, 2023)
* [#1014](https://github.com/awslabs/amazon-kinesis-client/pull/1014) Use AFTER_SEQUENCE_NUMBER iterator type for expired iterator request

### Release 2.4.4 (December 23, 2022)
* [#1017](https://github.com/awslabs/amazon-kinesis-client/pull/1017) Upgrade aws sdk 
  * aws-java-sdk.version from 1.12.296 -> 1.12.370
  * awssdk.version from 2.17.268 -> 2.19.2
* [#1020](https://github.com/awslabs/amazon-kinesis-client/pull/1020)  Correct the KCL version in the main pom

### Release 2.4.3 (September 6, 2022)
* [#980](https://github.com/awslabs/amazon-kinesis-client/pull/980) logback-classic: 1.2.9 -> 1.4.0
* [#983](https://github.com/awslabs/amazon-kinesis-client/pull/983)
  * protobuf-java: 3.19.2 -> 3.21.5
  * slf4j.version: 1.7.32 -> 2.0.0
  * schema-registry-serde: 1.1.9 -> 1.1.13
* [#984](https://github.com/awslabs/amazon-kinesis-client/pull/984) awssdk.version from 2.17.108 to 2.17.267
* [#987](https://github.com/awslabs/amazon-kinesis-client/pull/987) guava: 31.0.1-jre -> 31.1-jre
* [#988](https://github.com/awslabs/amazon-kinesis-client/pull/988) jcommander: 1.81 to 1.82
* [#990](https://github.com/awslabs/amazon-kinesis-client/pull/990)  Upgrade dependencies
  * aws-java-sdk.version: 1.12.130 -> 1.12.296
  * lombok: 1.18.22 -> 1.18.24
  * rxjava: 3.1.3 -> 3.1.5
  * maven-resources-plugin: 2.6 -> 3.3.0
  * logback-classic: 1.4.0 -> 1.3.0
  * awssdk.version: 2.17.267 -> 2.17.268

### Release 2.4.2 (August 10, 2022)
* [#972](https://github.com/awslabs/amazon-kinesis-client/pull/972) Upgrade Lombok to version 1.18.24

### Latest Release 2.4.1 (March 24, 2022)
[Milestone#68](https://github.com/awslabs/amazon-kinesis-client/milestone/68)
* [#916](https://github.com/awslabs/amazon-kinesis-client/pull/916) Upgrade to rxjava3

### Release 2.4.0 (March 2, 2022)
[Milestone#67](https://github.com/awslabs/amazon-kinesis-client/milestone/67)
* [#894](https://github.com/awslabs/amazon-kinesis-client/pull/894) Bump protobuf-java from 3.19.1 to 3.19.2
* [#924](https://github.com/awslabs/amazon-kinesis-client/pull/924) Support Protobuf Data format with Glue Schema Registry.

### Latest Release 2.3.10 (January 4, 2022)
[Milestone#66](https://github.com/awslabs/amazon-kinesis-client/milestone/66)
* [#868](https://github.com/awslabs/amazon-kinesis-client/pull/868) Adding a new metric: Application-level MillisBehindLatest
* [#879](https://github.com/awslabs/amazon-kinesis-client/pull/879) Keep dependencies up-to-date
* [#886](https://github.com/awslabs/amazon-kinesis-client/pull/886) Get latest counter before attempting a take to ensure take succeeds
* [#888](https://github.com/awslabs/amazon-kinesis-client/pull/888) Configure dependabot for v1.x branch

### Release 2.3.9 (November 22, 2021)
[Milestone#65](https://github.com/awslabs/amazon-kinesis-client/milestone/65)
* [#866](https://github.com/awslabs/amazon-kinesis-client/pull/866) Update logback dependency.

### Release 2.3.8 (October 27, 2021)
[Milestone#64](https://github.com/awslabs/amazon-kinesis-client/milestone/64)
* [#860](https://github.com/awslabs/amazon-kinesis-client/pull/860) Upgrade Glue schema registry from 1.1.4 to 1.1.5.
* [#861](https://github.com/awslabs/amazon-kinesis-client/pull/861) Revert [PR#847](https://github.com/awslabs/amazon-kinesis-client/pull/847) due to regression for leases without owners and added new tests.

### Release 2.3.7 (October 11, 2021)
[Milestone#63](https://github.com/awslabs/amazon-kinesis-client/milestone/63)
* [#842](https://github.com/awslabs/amazon-kinesis-client/pull/842) Fixing typo is debug logs.
* [#846](https://github.com/awslabs/amazon-kinesis-client/pull/846) Fix DynamoDBLeaseTaker logging of available leases
* [#847](https://github.com/awslabs/amazon-kinesis-client/pull/847) Make use of Java 8 to simplify computeLeaseCounts()
* [#853](https://github.com/awslabs/amazon-kinesis-client/pull/853) Add configurable initial position for orphaned stream
* [#854](https://github.com/awslabs/amazon-kinesis-client/pull/854) Create DynamoDB tables on On-Demand billing mode by default.
* [#855](https://github.com/awslabs/amazon-kinesis-client/pull/855) Emit Glue Schema Registry usage metrics
* [#857](https://github.com/awslabs/amazon-kinesis-client/pull/857) Fix to shutdown PrefetchRecordsPublisher in gracefull manner
* [#858](https://github.com/awslabs/amazon-kinesis-client/pull/858) Upgrade AWS SDK version to 2.17.52.

### Release 2.3.6 (July 9, 2021)
[Milestone#62](https://github.com/awslabs/amazon-kinesis-client/milestone/62)
* [#836](https://github.com/awslabs/amazon-kinesis-client/pull/836) Upgraded AWS SDK version to 2.16.98
* [#835](https://github.com/awslabs/amazon-kinesis-client/pull/835) Upgraded Glue Schema Registry version to 1.1.1
* [#828](https://github.com/awslabs/amazon-kinesis-client/pull/828) Modified wildcard imports to individual imports.
* [#817](https://github.com/awslabs/amazon-kinesis-client/pull/817) Updated the Worker shutdown logic to make sure that the `LeaseCleanupManager` also terminates all the threads that it has started.
* [#794](https://github.com/awslabs/amazon-kinesis-client/pull/794) Silence warning when there are no stale streams to delete.

### Release 2.3.5 (June 14, 2021)
[Milestone#59](https://github.com/awslabs/amazon-kinesis-client/milestone/59)
* [#824](https://github.com/awslabs/amazon-kinesis-client/pull/824) Upgraded dependencies
  * logback-classic version to 1.2.3
  * AWS Java SDK version to 1.12.3
  * AWS SDK version to 2.16.81
* [#815](https://github.com/awslabs/amazon-kinesis-client/pull/815) Converted Future to CompletableFuture which helps in proper conversion to Scala using Scala Future Converters.
* [#810](https://github.com/awslabs/amazon-kinesis-client/pull/810) Bump commons-io from 2.6 to 2.7
* [#804](https://github.com/awslabs/amazon-kinesis-client/pull/804) Allowing user to specify an initial timestamp in which daemon will process records.
* [#802](https://github.com/awslabs/amazon-kinesis-client/pull/802) Upgraded guava from 26.0-jre to 29.0-jre
* [#801](https://github.com/awslabs/amazon-kinesis-client/pull/801) Fixing a bug that causes to block indefinitely when trying to unlock a lock that isn't locked.
* [#762](https://github.com/awslabs/amazon-kinesis-client/pull/762) Added support for web identity token in multilang

### Release 2.3.4 (February 19, 2021)
[Milestone#56](https://github.com/awslabs/amazon-kinesis-client/milestone/56)
* [#788](https://github.com/awslabs/amazon-kinesis-client/pull/788) Fixing a bug that caused paginated `ListShards` calls with the `ShardFilter` parameter to fail when the lease table was being initialized.

### Release 2.3.3 (December 23, 2020)
[Milestone#55](https://github.com/awslabs/amazon-kinesis-client/milestone/55)
* Fixing bug in PrefetchRecordsPublisher which was causing retry storms if initial request fails.
* Fixing bug where idleTimeBetweenReadsInMillis property was ignored in PollingConfig.

### Release 2.3.2 (November 19, 2020)
[Milestone#54](https://github.com/awslabs/amazon-kinesis-client/milestone/54)
* Adding support for Glue Schema Registry. Deserialize and read schemas associated with the records.
* Updating AWS SDK version to 2.15.31


### Release 2.3.1 (October 20, 2020)
[Milestone#53](https://github.com/awslabs/amazon-kinesis-client/milestone/53)
* Introducing support for processing multiple kinesis data streams with the same KCL 2.x for java consumer application
  * To build a consumer application that can process multiple streams at the same time, you must implement a new
  interface called MultistreamTracker (https://github.com/awslabs/amazon-kinesis-client/blob/0c5042dadf794fe988438436252a5a8fe70b6b0b/amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/MultiStreamTracker.java)

  * MultistreamTracker will also publish various metrics around the current active streams being processed, the number
   of streams which are deleted at this time period or are pending deletion.


### Release 2.3.0 (August 17, 2020)
* [Milestone#52](https://github.com/awslabs/amazon-kinesis-client/milestones/52)

* Behavior of shard synchronization is moving from each worker independently learning about all existing shards to workers only discovering the children of shards that each worker owns. This optimizes memory usage, lease table IOPS usage, and number of calls made to kinesis for streams with high shard counts and/or frequent resharding.
* When bootstrapping an empty lease table, KCL utilizes the `ListShard` API's filtering option (the ShardFilter optional request parameter) to retrieve and create leases only for a snapshot of shards open at the time specified by the `ShardFilter` parameter. The `ShardFilter` parameter enables you to filter out the response of the `ListShards` API, using the `Type` parameter. KCL uses the `Type` filter parameter and the following of its valid values to identify and return a snapshot of open shards that might require new leases.
	* Currently, the following shard filters are supported:
		* `AT_TRIM_HORIZON` - the response includes all the shards that were open at `TRIM_HORIZON`.
		* `AT_LATEST` - the response includes only the currently open shards of the data stream.
	    * `AT_TIMESTAMP` - the response includes all shards whose start timestamp is less than or equal to the given timestamp and end timestamp is greater than or equal to the given timestamp or still open.
	* `ShardFilter` is used when creating leases for an empty lease table to initialize leases for a snapshot of shards specified at `RetrievalConfig#initialPositionInStreamExtended`.
	* For more information about ShardFilter, see the [official AWS documentation on ShardFilter](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ShardFilter.html).

* Introducing support for the `ChildShards` response of the `GetRecords` and the `SubscribeToShard` APIs to perform lease/shard synchronization that happens at `SHARD_END` for closed shards, allowing a KCL worker to only create leases for the child shards of the shard it finished processing.
	* For shared throughout consumer applications, this uses the `ChildShards` response of the `GetRecords` API. For dedicated throughput (enhanced fan-out) consumer applications, this uses the `ChildShards` response of the `SubscribeToShard` API.
	* For more information, see the official AWS Documentation on [GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html), [SubscribeToShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html), and [ChildShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ChildShard.html).

* KCL now also performs additional periodic shard/lease scans in order to identify any potential holes in the lease table to ensure the complete hash range of the stream is being processed and create leases for them if required. `PeriodicShardSyncManager` is the new component that is responsible for running periodic lease/shard scans.
	* New configuration options are available to configure `PeriodicShardSyncManager` in `LeaseManagementConfig`

	| Name                                                  | Default            | Description                                                                                                                                                                                                                                                   |
	| ----------------------------------------------------- | -----------------  | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
	| leasesRecoveryAuditorExecutionFrequencyMillis         | 120000 (2 minutes) | Frequency (in millis) of the auditor job to scan for partial leases in the lease table. If the auditor detects any hole in the leases for a stream, then it would trigger shard sync based on leasesRecoveryAuditorInconsistencyConfidenceThreshold.          |
	| leasesRecoveryAuditorInconsistencyConfidenceThreshold | 3                  | Confidence threshold for the periodic auditor job to determine if leases for a stream in the lease table is inconsistent. If the auditor finds same set of inconsistencies consecutively for a stream for this many times, then it would trigger a shard sync |

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

* Introducing _experimental_ support for multistreaming, allowing a single KCL application to multiplex processing multiple streams.
	* New configuration options are available to enable multistreaming in `RetrievalConfig#appStreamTracker`.

* Fixing a bug in `PrefetchRecordsPublisher` restarting while it was already running.
* Including an optimization to `HierarchicalShardSyncer` to only create leases for one layer of shards.
* Adding support to prepare and commit lease checkpoints with arbitrary bytes.
	* This allows checkpointing of an arbitrary byte buffer up to the maximum permitted DynamoDB item size ([currently 400 KB as of release](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)), and can be used for recovery by passing a serialized byte buffer to `RecordProcessorCheckpointer#prepareCheckpoint` and `RecordProcessorCheckpointer#checkpoint`.
* Upgrading version of AWS SDK to 2.14.0.
* [#725](https://github.com/awslabs/amazon-kinesis-client/pull/725) Allowing KCL to consider lease tables in `UPDATING` healthy.

### Release 2.2.11 (May 28, 2020)
[Milestone#51](https://github.com/awslabs/amazon-kinesis-client/milestone/51)
* Adjusting HTTP2 initial window size to 512 KB
  * [PR#706](https://github.com/awslabs/amazon-kinesis-client/pull/706)
* Updating protobuf-java to version 3.11.4
  * [PR#718](https://github.com/awslabs/amazon-kinesis-client/pull/718)
* Updating the AWS Java SDK to version 2.13.25
  * [PR#722](https://github.com/awslabs/amazon-kinesis-client/pull/722)
  
### Release 2.2.10 (March 26, 2020)
[Milestone#48](https://github.com/awslabs/amazon-kinesis-client/milestone/48)
* Fixing a bug in DynamoDB billing mode support for special regions.
  * [PR#703](https://github.com/awslabs/amazon-kinesis-client/pull/703)
* Adding request id logging to ShardConsumerSubscriber.
  * [PR#705](https://github.com/awslabs/amazon-kinesis-client/pull/705)

### Release 2.2.9 (Febuary 17, 2020)
[Milestone#47](https://github.com/awslabs/amazon-kinesis-client/milestone/47)
* Updating the AWS SDK version to 2.10.66.
  * [PR#687](https://github.com/awslabs/amazon-kinesis-client/commit/8aaf2aa11c43f77f459732cdb7d88f4418d367ff)
* Adding request id logging to SubscribeToShard response.
  * [PR#678](https://github.com/awslabs/amazon-kinesis-client/pull/678)

### Release 2.2.8 (January 28, 2020)
[Milestone#46](https://github.com/awslabs/amazon-kinesis-client/milestone/45)
* Updating the AWS SDK version to 2.10.56.
  * [PR#679](https://github.com/awslabs/amazon-kinesis-client/pull/679)
  * NOTE: SDK has a known connection teardown issue when multiple H2 streams are used within a connection. This might result in shard consumers sticking to a stale service host and not progressing. If your shard consumer gets stuck, use the following configuration as a workaround. This configuration might result in up to 5X increase in total connections.
  ```
  KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder()
                                                       .region(region)
                                                       .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(Integer.MAX_VALUE).http2Configuration(Http2Configuration.builder().maxStreams(1).build())
                                                       .build()
  ```
* Making ShardConsumerTest resilient to race conditions.
  * [PR#668](https://github.com/awslabs/amazon-kinesis-client/pull/668)
* Updating integration test naming.
  * [PR#667](https://github.com/awslabs/amazon-kinesis-client/pull/667)

### Release 2.2.7 (December 2, 2019)
[Milestone#45](https://github.com/awslabs/amazon-kinesis-client/milestone/45)
* Updating the AWS SDK version to 2.10.25
  * [PR#657](https://github.com/awslabs/amazon-kinesis-client/pull/657)
* Adding a configurable DynamoDB billing mode
  * [PR#582](https://github.com/awslabs/amazon-kinesis-client/pull/582)
  * NOTE: Billing mode is not available in all regions; if your lease table cannot be created, use the following configuration as a workaround:
  ```
  LeaseManagementConfig leaseManagementConfig = builder.leaseManagementConfig().billingMode(null).build();
  ```


### Release 2.2.6 (November 7, 2019)
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

### Release 2.2.5 (October 23, 2019)

[Milestone#40](https://github.com/awslabs/amazon-kinesis-client/milestone/40)
* Updating Sonatype to dedicated AWS endpoint.
  * [PR#619](https://github.com/awslabs/amazon-kinesis-client/pull/619)
* Introducing a validation step to verify if ShardEnd is reached, to prevent shard consumer stuck scenarios in the event of malformed response from service.
  * [PR#624](https://github.com/awslabs/amazon-kinesis-client/pull/624)

### Release 2.2.4 (September 23, 2019)

[Milestone#39](https://github.com/awslabs/amazon-kinesis-client/milestone/39)
* Making FanoutRecordsPublisher test cases resilient to delayed thread operations
  * [PR#612](https://github.com/awslabs/amazon-kinesis-client/pull/612)
* Drain delivery queue in the FanoutRecordsPublisher to make slow consumers consume events at their pace
  * [PR#607](https://github.com/awslabs/amazon-kinesis-client/pull/607)
* Fix to prevent the onNext event going to stale subscription when restart happens in PrefetchRecordsPublisher
  * [PR#606](https://github.com/awslabs/amazon-kinesis-client/pull/606)

### Release 2.2.3 (September 04, 2019)

[Milestone#38](https://github.com/awslabs/amazon-kinesis-client/milestone/38)
* Fix to prevent data loss and stuck shards in the event of failed records delivery in Polling readers
  * [PR#603](https://github.com/awslabs/amazon-kinesis-client/pull/603)

### Release 2.2.2 (August 19, 2019)

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

### Release 2.2.1 (July 1, 2019)
[Milestone#32](https://github.com/awslabs/amazon-kinesis-client/milestone/32)
* Add periodic logging for the state of the thread pool executor service. This service executes the async tasks submitted to and by the ShardConsumer.
* Add logging of failures from RxJava layer.
  * [PR#559](https://github.com/awslabs/amazon-kinesis-client/pull/559)

### Release 2.2.0 (April 8, 2019)
[Milestone#31](https://github.com/awslabs/amazon-kinesis-client/milestone/31)
* Updated License to [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
  * [PR#523](https://github.com/awslabs/amazon-kinesis-client/pull/523)
* Introducing configuration for suppressing logs from ReadTimeoutExceptions caused while calling SubscribeToShard.  
Suppression can be configured by setting `LifecycleConfig#readTimeoutsToIgnoreBeforeWarning(Count)`.
  * [PR#528](https://github.com/awslabs/amazon-kinesis-client/issues/528)

### Release 2.1.3 (March 18, 2019)
[Milestone#30](https://github.com/awslabs/amazon-kinesis-client/milestone/30)
* Added a message to recommend using `KinesisClientUtil` when an acquire timeout occurs in the `FanOutRecordsPublisher`.
  * [PR#514](https://github.com/awslabs/amazon-kinesis-client/pull/514)
* Added a sleep between retries while waiting for a newly created stream consumer to become active.
  * [PR#506](https://github.com/awslabs/amazon-kinesis-client/issues/506)
* Added timeouts on all futures returned from the DynamoDB and Kinesis clients.  
  The timeouts can be configured by setting `LeaseManagementConfig#requestTimeout(Duration)` for DynamoDB, and `PollingConfig#kinesisRequestTimeout(Duration)` for Kinesis.
  * [PR#518](https://github.com/awslabs/amazon-kinesis-client/pull/518)
* Upgraded to SDK version 2.5.10.
  * [PR#518](https://github.com/awslabs/amazon-kinesis-client/pull/518)
* Artifacts for the Amazon Kinesis Client for Java are now signed by a new GPG key:
  ```
  pub   4096R/86368934 2019-02-14 [expires: 2020-02-14]
  uid                  Amazon Kinesis Tools <amazon-kinesis-tools@amazon.com>
  ```

### Release 2.1.2 (February 18, 2019)
[Milestone#29](https://github.com/awslabs/amazon-kinesis-client/milestone/29)
* Fixed handling of the progress detection in the `ShardConsumer` to restart from the last accepted record, instead of the last queued record.
  * [PR#492](https://github.com/awslabs/amazon-kinesis-client/pull/492)
* Fixed handling of exceptions when using polling so that it will no longer treat `SdkException`s as an unexpected exception.
  * [PR#497](https://github.com/awslabs/amazon-kinesis-client/pull/497)
  * [PR#502](https://github.com/awslabs/amazon-kinesis-client/pull/502)
* Fixed a case where lease loss would block the `Scheduler` while waiting for a record processor's `processRecords` method to complete.
  * [PR#501](https://github.com/awslabs/amazon-kinesis-client/pull/501)

### Release 2.1.1 (February 6, 2019)
[Milestone#28](https://github.com/awslabs/amazon-kinesis-client/milestone/28)
* Introducing `SHUT_DOWN_STARTED` state for the `WorkerStateChangeListener`.
  * [PR#457](https://github.com/awslabs/amazon-kinesis-client/pull/457)
* Fixed a bug with `AWSSessionCredentials` using `AWSSecretID` instead of `AWSAccessID` and vice versa.
  * [PR#486](https://github.com/awslabs/amazon-kinesis-client/pull/486)
* Upgrading SDK version to 2.4.0, which includes a fix for a possible deadlock when using Enhanced Fan-Out.
  * [PR#493](https://github.com/awslabs/amazon-kinesis-client/pull/493)

### Release 2.1.0 (January 14, 2019)
[Milestone #27](https://github.com/awslabs/amazon-kinesis-client/milestone/27)
* Introducing MultiLangDaemon support for Enhanced Fan-Out.  
* MultiLangDaemon now supports the following command line options.
  * `--properties-file`: Properties file that the KCL should use to set up the Scheduler.
  * `--log-configuration`: logback.xml that the KCL should use for logging.
* Updated AWS SDK dependency to 2.2.0.
* MultiLangDaemon now uses logback for logging.

### Release 2.0.5 (November 12, 2018)
[Milestone #26](https://github.com/awslabs/amazon-kinesis-client/milestone/26?closed=1)
* Fixed a deadlock condition that could occur when using the polling model.  
  When using the `PollingConfig` and a slower record processor it was possible to hit a deadlock in the retrieval of records. 
  * [PR #462](https://github.com/awslabs/amazon-kinesis-client/pull/462)
  * [Issue #448](https://github.com/awslabs/amazon-kinesis-client/issues/448)
* Adjusted `RetrievalConfig`, and `FanOutConfig` to use accessors instead of direct member access.  
  * [PR #453](https://github.com/awslabs/amazon-kinesis-client/pull/453)


### Release 2.0.4 (October 18, 2018)
[Milestone #25](https://github.com/awslabs/amazon-kinesis-client/milestone/25)
* Added method to retrieve leases from the LeaseCoordinator and LeaseTaker.
  * [PR #428](https://github.com/awslabs/amazon-kinesis-client/pull/428)
* Fixed a race condition shutting down the Scheduler before it has completed initialization.
  * [PR #439](https://github.com/awslabs/amazon-kinesis-client/pull/439)
  * [Issue #427](https://github.com/awslabs/amazon-kinesis-client/issues/427)
* Added `HierarchicalShardSyncer` which replaces the static `ShardSyncer`.  
  `HierarchicalShardSyncer` removes the contention between multiple instances of the Scheduler when running under a single JVM.
  * [PR #395](https://github.com/awslabs/amazon-kinesis-client/pull/395)
  * [Issue #415](https://github.com/awslabs/amazon-kinesis-client/issues/415)
* Added `TaskExecutionListener` which allows monitoring of tasks being executed by the `ShardConsumer`.  
  The listener is invoked before and after a task is executed by the `ShardConsumer`.
  * [PR #417](https://github.com/awslabs/amazon-kinesis-client/pull/417)

### Release 2.0.3 (October 8, 2018)
[Milestone #23](https://github.com/awslabs/amazon-kinesis-client/milestone/23)
* Fixed an issue where the `KinesisAsyncClient` could be misconfigured to use HTTP 1.1.  
  Using HTTP 1.1 with `SubscribeToShard` is unsupported, and could cause misdelivery of records to the record processor.  
  * [Issue #391](https://github.com/awslabs/amazon-kinesis-client/issues/391)
  * [PR #434](https://github.com/awslabs/amazon-kinesis-client/pull/434)
  * [PR #433](https://github.com/awslabs/amazon-kinesis-client/pull/433)
* Lower the severity of `ReadTimeout` exceptions.  
  `ReadTimeout` exceptions can occur if the client is unable to request data from Kinesis for more than client timeout, which defaults to 30 seconds.  This can occur if the record processor blocks for more than the timeout period.  `ReadTimeout` could also occur as part of [Issue #391](https://github.com/awslabs/amazon-kinesis-client/issues/391).  
  * [Issue #399](https://github.com/awslabs/amazon-kinesis-client/issues/399)
  * [PR #403](https://github.com/awslabs/amazon-kinesis-client/pull/403)
* Added a callback that allows applications to take actions after DynamoDB table creation.  
  Applications can now install a callback that is called after creating the DynamoDB table by implementing `TableCreatorCallback`.  
  * [PR #413](https://github.com/awslabs/amazon-kinesis-client/pull/413)
* Updated the guava dependency to 26.0-jre.  
  * [PR #420](https://github.com/awslabs/amazon-kinesis-client/pull/420)
  * [Issue #416](https://github.com/awslabs/amazon-kinesis-client/issues/416)
* Added some additional debug logging around the initialization of the `FanOutRecordsPublisher`.  
  * [PR #398](https://github.com/awslabs/amazon-kinesis-client/pull/398)
* Upgraded AWS SDK version to 2.0.6  
  * [PR #434](https://github.com/awslabs/amazon-kinesis-client/pull/434)


### Release 2.0.2 (September 4, 2018)
[Milestone #22](https://github.com/awslabs/amazon-kinesis-client/milestone/22)
* Fixed an issue where the a warning would be logged every second if `logWarningForTaskAfterMillis` was set.  
  The logging for last time of data arrival now respects the value of `logWarningForTaskAfterMillis`.  
  * [PR #383](https://github.com/awslabs/amazon-kinesis-client/pull/383)
  * [Issue #381](https://github.com/awslabs/amazon-kinesis-client/issues/381)
* Moved creation of `WorkerStateChangedListener` and `GracefulShutdownCoordinator` to the `CoordinatorConfig`.
  Originally the `WorkerStateChangedListener` and `GracefulShutdownCoordinator` were created by methods on the `SchedulerCoordinatorFactory`, but they should have been configuration options.  
  The original methods have been deprecated, and may be removed at a later date.  
  * [PR #385](https://github.com/awslabs/amazon-kinesis-client/pull/385)
  * [PR #388](https://github.com/awslabs/amazon-kinesis-client/pull/388)
* Removed dependency on Apache Commons Lang 2.6.  
  The dependency on Apache Commons Lang 2.6 has removed, and all usages updated to use Apache Commons Lang 3.7.  
  * [PR #386](https://github.com/awslabs/amazon-kinesis-client/pull/386)
  * [Issue #370](https://github.com/awslabs/amazon-kinesis-client/issues/370)
* Fixed a typo in the MutliLang Daemon shutdown hook.  
  * [PR #387](https://github.com/awslabs/amazon-kinesis-client/pull/387)
* Added method `onAllInitializationAttemptsFailed(Throwable)` to `WorkerStateChangedListener` to report when all initialization attempts have failed.  
  This method is a default method, and it isn't require to implement the method. This method is only called after all attempts to initialize the `Scheduler` have failed.
  * [PR #369](https://github.com/awslabs/amazon-kinesis-client/pull/369)

### Release 2.0.1 (August 21, 2018)
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

### Release 2.0.0 (August 02, 2018)
* The Maven `groupId`, along with the `version`, for the Amazon Kinesis Client has changed from `com.amazonaws` to `software.amazon.kinesis`.  
  To add a dependency on the new version of the Amazon Kinesis Client:  
  ``` xml
  <dependency>
      <groupId>software.amazon.kinesis</groupId>
      <artifactId>amazon-kinesis-client</artifactId>
      <version>2.0.0</version>
  </dependency>
  ```
* Added support for Enhanced Fan Out.  
  Enhanced Fan Out provides for lower end to end latency, and increased number of consumers per stream. 
  * Records are now delivered via streaming, reducing end-to-end latency.
  * The Amazon Kinesis Client will automatically register a new consumer if required.  
    When registering a new consumer, the Kinesis Client will default to the application name unless configured otherwise.
  * `SubscribeToShard` maintains long lived connections with Kinesis, which in the AWS Java SDK 2.0 is limited by default.  
    The `KinesisClientUtil` has been added to assist configuring the `maxConcurrency` of the `KinesisAsyncClient`.   
    __WARNING: The Amazon Kinesis Client may see significantly increased latency, unless the `KinesisAsyncClient` is configured to have a `maxConcurrency` high enough to allow all leases plus additional usages of the `KinesisAsyncClient`.__
  * The Amazon Kinesis Client now uses 3 additional Kinesis API's:  
    __WARNING: If using a restrictive Kinesis IAM policy you may need to add the following API methods to the policy.__  
    * [`SubscribeToShard`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html)
    * [`DescribeStreamSummary`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html)
    * [`DescribeStreamConsumer`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamConsumer.html)
    * [`RegisterStreamConsumer`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html)
  * New configuration options are available to configure Enhanced Fan Out.  
  
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
