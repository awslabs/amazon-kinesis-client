FROM maven:3.9-amazoncorretto-17 AS build
# SigV4 OTLP exporter - v3
WORKDIR /app
COPY pom.xml .
COPY .m2/repository /root/.m2/repository
RUN mvn dependency:go-offline -B || true
COPY src ./src
RUN mvn package -DskipTests -q

FROM amazoncorretto:17-alpine
RUN apk add --no-cache curl
WORKDIR /app
COPY --from=build /app/target/kcl-otel-sample-app-1.0.0.jar app.jar

# Download ADOT Java agent for SigV4-authenticated OTLP export to CloudWatch
RUN curl -L -o aws-opentelemetry-agent.jar \
    https://github.com/aws-observability/aws-otel-java-instrumentation/releases/latest/download/aws-opentelemetry-agent.jar

ENTRYPOINT ["java", "-javaagent:aws-opentelemetry-agent.jar", "-jar", "app.jar"]
