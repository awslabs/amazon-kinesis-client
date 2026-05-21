package software.amazon.kinesis.sample.infra;

import java.util.List;
import java.util.Map;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ecr.assets.DockerImageAsset;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateService;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.LogDrivers;
import software.amazon.awscdk.services.ecs.PortMapping;
import software.amazon.awscdk.services.ecs.Protocol;
import software.amazon.awscdk.services.grafana.CfnWorkspace;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.kinesis.StreamMode;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

/**
 * CDK Stack that provisions the full KCL OTel sample application:
 * - Kinesis Data Stream (on-demand)
 * - DynamoDB lease table
 * - ECS Fargate cluster + service running the KCL consumer
 * - IAM roles with least-privilege permissions
 * - Amazon Managed Grafana workspace for visualization
 */
public class KclOtelSampleStack extends Stack {

    private static final String APP_NAME = "kcl-otel-sample";
    private static final String STREAM_NAME = APP_NAME + "-stream";
    private static final String LEASE_TABLE_NAME = APP_NAME + "-leases";

    public KclOtelSampleStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        String region = this.getRegion();

        // --- Networking ---
        Vpc vpc = Vpc.Builder.create(this, "Vpc")
                .maxAzs(2)
                .natGateways(1)
                .build();

        // --- Kinesis Data Stream ---
        Stream kinesisStream = Stream.Builder.create(this, "KinesisStream")
                .streamName(STREAM_NAME)
                .streamMode(StreamMode.ON_DEMAND)
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        // --- DynamoDB Lease Table ---
        Table leaseTable = Table.Builder.create(this, "LeaseTable")
                .tableName(LEASE_TABLE_NAME)
                .partitionKey(Attribute.builder()
                        .name("leaseKey")
                        .type(AttributeType.STRING)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        // --- ECS Cluster ---
        Cluster cluster = Cluster.Builder.create(this, "EcsCluster")
                .vpc(vpc)
                .clusterName(APP_NAME + "-cluster")
                .containerInsights(Boolean.TRUE)
                .build();

        // --- Task Execution Role (pull images, write logs) ---
        Role taskExecutionRole = Role.Builder.create(this, "TaskExecutionRole")
                .assumedBy(new ServicePrincipal("ecs-tasks.amazonaws.com"))
                .managedPolicies(List.of(
                        ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy")))
                .build();

        // --- Task Role (app permissions: Kinesis, DDB, CloudWatch OTLP) ---
        Role taskRole = Role.Builder.create(this, "TaskRole")
                .assumedBy(new ServicePrincipal("ecs-tasks.amazonaws.com"))
                .build();

        // Kinesis read permissions
        kinesisStream.grantRead(taskRole);
        taskRole.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "kinesis:SubscribeToShard",
                        "kinesis:RegisterStreamConsumer",
                        "kinesis:DescribeStreamConsumer"))
                .resources(List.of(kinesisStream.getStreamArn(), kinesisStream.getStreamArn() + "/*"))
                .build());

        // DynamoDB lease table permissions (KCL creates its own table using app name)
        taskRole.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "dynamodb:CreateTable",
                        "dynamodb:DescribeTable",
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:Scan",
                        "dynamodb:Query",
                        "dynamodb:UpdateTable",
                        "dynamodb:DescribeTimeToLive",
                        "dynamodb:UpdateTimeToLive",
                        "dynamodb:TagResource"))
                .resources(List.of("*"))
                .build());

        // CloudWatch OTLP endpoint permissions
        taskRole.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "cloudwatch:PutMetricData",
                        "xray:PutTelemetryRecords",
                        "xray:PutTraceSegments",
                        "xray:GetSamplingRules",
                        "xray:GetSamplingTargets"))
                .resources(List.of("*"))
                .build());

        // CloudWatch metrics read (for verification)
        taskRole.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "cloudwatch:GetMetricData",
                        "cloudwatch:ListMetrics"))
                .resources(List.of("*"))
                .build());

        // --- Log Group ---
        LogGroup logGroup = LogGroup.Builder.create(this, "LogGroup")
                .logGroupName("/ecs/" + APP_NAME)
                .retention(RetentionDays.ONE_WEEK)
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        // --- Fargate Task Definition ---
        FargateTaskDefinition taskDef = FargateTaskDefinition.Builder.create(this, "TaskDef")
                .memoryLimitMiB(1024)
                .cpu(512)
                .executionRole(taskExecutionRole)
                .taskRole(taskRole)
                .runtimePlatform(software.amazon.awscdk.services.ecs.RuntimePlatform.builder()
                        .cpuArchitecture(software.amazon.awscdk.services.ecs.CpuArchitecture.ARM64)
                        .operatingSystemFamily(software.amazon.awscdk.services.ecs.OperatingSystemFamily.LINUX)
                        .build())
                .build();

        String otlpEndpoint = "https://otlp." + region + ".amazonaws.com/v1/metrics";

        // Build Docker image from the app directory (ARM64 for Graviton Fargate)
        DockerImageAsset appImage = DockerImageAsset.Builder.create(this, "AppImage")
                .directory("../app")
                .platform(software.amazon.awscdk.services.ecr.assets.Platform.LINUX_ARM64)
                .build();

        Map<String, String> containerEnv = new java.util.HashMap<>();
        containerEnv.put("STREAM_NAME", STREAM_NAME);
        containerEnv.put("APPLICATION_NAME", APP_NAME);
        containerEnv.put("AWS_REGION", region);
        containerEnv.put("OTLP_ENDPOINT", otlpEndpoint);
        containerEnv.put("EXPORT_INTERVAL_MILLIS", "60000");
        containerEnv.put("LEASE_TABLE_NAME", LEASE_TABLE_NAME);
        // Disable the auto-injected OTel Java agent — we manage our own SDK
        containerEnv.put("JAVA_TOOL_OPTIONS", "");
        containerEnv.put("OTEL_JAVAAGENT_ENABLED", "false");
        containerEnv.put("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", "false");

        taskDef.addContainer("KclApp", ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromDockerImageAsset(appImage))
                .logging(LogDrivers.awsLogs(software.amazon.awscdk.services.ecs.AwsLogDriverProps.builder()
                        .logGroup(logGroup)
                        .streamPrefix("kcl")
                        .build()))
                .environment(containerEnv)
                .build());

        // --- ECS Fargate Service ---
        FargateService.Builder.create(this, "FargateService")
                .cluster(cluster)
                .taskDefinition(taskDef)
                .desiredCount(1)
                .serviceName(APP_NAME + "-service")
                .assignPublicIp(false)
                .build();

        // --- Amazon Managed Grafana (commented out - account quota exceeded) ---
        // To enable: request quota increase for Grafana workspaces in your region,
        // then uncomment the block below.
        /*
        Role grafanaRole = Role.Builder.create(this, "GrafanaRole")
                .assumedBy(new ServicePrincipal("grafana.amazonaws.com"))
                .build();
        grafanaRole.addManagedPolicy(
                ManagedPolicy.fromAwsManagedPolicyName("CloudWatchReadOnlyAccess"));
        grafanaRole.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("aps:QueryMetrics", "aps:GetLabels", "aps:GetSeries", "aps:GetMetricMetadata"))
                .resources(List.of("*"))
                .build());
        CfnWorkspace grafanaWorkspace = CfnWorkspace.Builder.create(this, "GrafanaWorkspace")
                .name(APP_NAME + "-grafana")
                .description("KCL OTel Sample - Metrics Visualization")
                .accountAccessType("CURRENT_ACCOUNT")
                .authenticationProviders(List.of("AWS_SSO"))
                .permissionType("SERVICE_MANAGED")
                .dataSources(List.of("CLOUDWATCH", "PROMETHEUS"))
                .roleArn(grafanaRole.getRoleArn())
                .build();
        */

        // --- Outputs ---
        CfnOutput.Builder.create(this, "StreamName")
                .value(kinesisStream.getStreamName())
                .description("Kinesis Data Stream name")
                .build();

        CfnOutput.Builder.create(this, "LeaseTableName")
                .value(leaseTable.getTableName())
                .description("DynamoDB lease table name")
                .build();

        CfnOutput.Builder.create(this, "OtlpEndpoint")
                .value(otlpEndpoint)
                .description("CloudWatch OTLP endpoint for metrics export")
                .build();

        CfnOutput.Builder.create(this, "EcsClusterName")
                .value(cluster.getClusterName())
                .description("ECS cluster name")
                .build();
    }
}
