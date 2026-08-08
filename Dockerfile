FROM eclipse-temurin:21-jdk AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY .mvn .mvn
COPY mvnw mvnw
COPY pom.xml pom.xml
COPY scheduler-contracts scheduler-contracts
COPY scheduler-client scheduler-client
COPY scheduler-service scheduler-service
RUN chmod +x mvnw \
    && ./mvnw -B -ntp -pl scheduler-service -am -DskipTests package

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S scheduler && adduser -S scheduler -G scheduler
WORKDIR /app
COPY --from=build /workspace/scheduler-service/target/scheduler-service-*.jar /app/app.jar
USER scheduler

EXPOSE 8080 9090
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
