package software.amazon.kinesis.sample.infra;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

public class InfraApp {
    public static void main(final String[] args) {
        App app = new App();

        String account = (String) app.getNode().tryGetContext("account");
        String region = (String) app.getNode().tryGetContext("region");

        Environment env = Environment.builder()
                .account(account != null ? account : System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(region != null ? region : System.getenv("CDK_DEFAULT_REGION"))
                .build();

        new KclOtelSampleStack(app, "KclOtelSampleStack", StackProps.builder()
                .env(env)
                .description("KCL OTel Sample App - Kinesis stream, ECS Fargate, CloudWatch OTLP metrics, Managed Grafana")
                .build());

        app.synth();
    }
}
