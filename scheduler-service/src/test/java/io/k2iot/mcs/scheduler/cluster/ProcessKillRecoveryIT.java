package io.k2iot.mcs.scheduler.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.k2iot.mcs.scheduler.execution.ExecutionIdentity;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class ProcessKillRecoveryIT {

  private static final Duration CLUSTER_CHECK_IN = Duration.ofSeconds(2);
  private static final Duration MISFIRE_THRESHOLD = Duration.ofSeconds(3);
  private static final Duration RECOVERY_WAIT = Duration.ofSeconds(35);
  private static final String NAMESPACE = "task15";
  private static final String COMMAND_TOPIC = "mcs.scheduler.task15.commands";
  private static final String COMMAND_RESULT_TOPIC = "mcs.scheduler.task15.command-results";
  private static final String DLT_TOPIC = "mcs.scheduler.task15.commands.DLT";

  @Test
  void survivingNodeRecoversOneLogicalExecutionAfterAcquiringNodeIsKilled() throws Exception {
    Path repositoryRoot = repositoryRoot();
    Path testDockerfile =
        repositoryRoot.resolve("scheduler-service/src/test/resources/cluster/Dockerfile.test");
    Path serviceJar = serviceJar(repositoryRoot);

    assertThat(testDockerfile).as("Task 15 containerized recovery Dockerfile").isRegularFile();
    assertThat(serviceJar).as("packaged scheduler-service executable jar").isRegularFile();

    ImageFromDockerfile schedulerImage =
        new ImageFromDockerfile("mcs-scheduler-process-kill:" + UUID.randomUUID(), true)
            .withFileFromPath("Dockerfile", testDockerfile)
            .withFileFromPath("app.jar", serviceJar);

    try (Network network = Network.newNetwork();
        PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("scheduler")
                .withUsername("scheduler")
                .withPassword("scheduler")
                .withNetwork(network)
                .withNetworkAliases("postgres");
        KafkaContainer kafka =
            new KafkaContainer("apache/kafka-native:3.8.0")
                .withListener("kafka:19092")
                .withNetwork(network);
        GenericContainer<?> nodeA =
            schedulerNode(schedulerImage, network, postgres, kafka, "task15-node-a");
        GenericContainer<?> nodeB =
            schedulerNode(schedulerImage, network, postgres, kafka, "task15-node-b")) {
      postgres.start();
      kafka.start();
      createTopic(kafka, COMMAND_TOPIC);
      createTopic(kafka, COMMAND_RESULT_TOPIC);
      createTopic(kafka, DLT_TOPIC);

      nodeA.start();
      nodeB.start();

      createAcquisitionMarkerTable(postgres);
      String eventTopic = "mcs.scheduler.task15.executions." + UUID.randomUUID();
      createTopic(kafka, eventTopic);

      UUID destinationId = UUID.randomUUID();
      UUID jobId = UUID.randomUUID();
      UUID triggerId = UUID.randomUUID();
      registerDestination(postgres, destinationId, eventTopic);

      try (Consumer<String, String> consumer = executionConsumer(kafka, eventTopic)) {
        Instant fireAt = Instant.now().plusSeconds(10).truncatedTo(ChronoUnit.MILLIS);
        createRecoverySchedule(nodeA, destinationId, jobId, triggerId, fireAt);

        String acquiredBy = awaitAcquiredNode(postgres);
        assertThat(acquiredBy).isIn("task15-node-a", "task15-node-b");

        GenericContainer<?> acquiredNode = acquiredBy.equals("task15-node-a") ? nodeA : nodeB;
        GenericContainer<?> survivingNode = acquiredNode == nodeA ? nodeB : nodeA;
        assertThat(survivingNode.isRunning()).isTrue();

        DockerClientFactory.instance()
            .client()
            .killContainerCmd(acquiredNode.getContainerId())
            .exec();

        await("acquired scheduler process is killed")
            .atMost(Duration.ofSeconds(10))
            .until(() -> !acquiredNode.isRunning());

        UUID expectedExecutionId = ExecutionIdentity.forScheduled(triggerId, fireAt);
        List<UUID> observedExecutionIds = new ArrayList<>();
        JsonMapper jsonMapper = JsonMapper.builder().findAndAddModules().build();

        await("surviving Quartz node recovers the firing")
            .pollInSameThread()
            .pollInterval(Duration.ofMillis(250))
            .atMost(RECOVERY_WAIT)
            .untilAsserted(
                () -> {
                  drainExecutionIds(consumer, jsonMapper, observedExecutionIds);
                  assertThat(executionCount(postgres, expectedExecutionId)).isEqualTo(1);
                  assertThat(executionRecoveryFlag(postgres, expectedExecutionId)).isTrue();
                  assertThat(observedExecutionIds).contains(expectedExecutionId);
                });

        await("recovery remains logically exactly-once after failover settles")
            .pollInSameThread()
            .pollInterval(Duration.ofMillis(250))
            .during(CLUSTER_CHECK_IN.multipliedBy(2).plus(MISFIRE_THRESHOLD))
            .atMost(Duration.ofSeconds(15))
            .untilAsserted(
                () -> {
                  drainExecutionIds(consumer, jsonMapper, observedExecutionIds);
                  assertThat(executionCount(postgres, expectedExecutionId)).isEqualTo(1);
                  assertThat(duplicateLogicalExecutionCount(postgres)).isZero();
                  assertThat(observedExecutionIds).containsExactly(expectedExecutionId);
                });
      }
    }
  }

  private static GenericContainer<?> schedulerNode(
      ImageFromDockerfile image,
      Network network,
      PostgreSQLContainer<?> postgres,
      KafkaContainer kafka,
      String instanceId) {
    return new GenericContainer<>(image)
        .withNetwork(network)
        .withExposedPorts(8080)
        .dependsOn(postgres, kafka)
        .withEnv("SPRING_PROFILES_ACTIVE", "cluster-recovery-test")
        .withEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/scheduler")
        .withEnv("SPRING_DATASOURCE_USERNAME", "scheduler")
        .withEnv("SPRING_DATASOURCE_PASSWORD", "scheduler")
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        .withEnv("MCS_SCHEDULER_INSTANCE_ID", instanceId)
        .withEnv("MCS_SCHEDULER_KAFKA_COMMAND_TOPIC", COMMAND_TOPIC)
        .withEnv("MCS_SCHEDULER_KAFKA_COMMAND_RESULT_TOPIC", COMMAND_RESULT_TOPIC)
        .withEnv("MCS_SCHEDULER_KAFKA_DLT_TOPIC", DLT_TOPIC)
        .withEnv("MCS_SCHEDULER_KAFKA_CONSUMER_GROUP", "mcs-scheduler-task15")
        .withEnv("MCS_SCHEDULER_KAFKA_PARTITIONS", "1")
        .withEnv("MCS_SCHEDULER_KAFKA_REPLICAS", "1")
        .withEnv("MCS_SCHEDULER_KAFKA_RETRY_ATTEMPTS", "0")
        .withEnv("MCS_SCHEDULER_OUTBOX_ENABLED", "true")
        .withEnv("MCS_SCHEDULER_OUTBOX_POLL_INTERVAL", "100ms")
        .withEnv("MCS_SCHEDULER_OUTBOX_CLAIM_TIMEOUT", "2s")
        .withEnv("MCS_SCHEDULER_OUTBOX_PUBLISH_TIMEOUT", "5s")
        .withEnv("MCS_SCHEDULER_TEST_RECOVERY_HOOK_ENABLED", "true")
        .waitingFor(
            Wait.forHttp("/actuator/health")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private static void createRecoverySchedule(
      GenericContainer<?> node, UUID destinationId, UUID jobId, UUID triggerId, Instant fireAt)
      throws IOException, InterruptedException {
    String body =
        """
        {
          "caller": "task-15-test",
          "job": {
            "jobId": "%s",
            "namespace": "%s",
            "name": "process-kill-%s",
            "description": "Task 15 process-kill recovery",
            "destinationId": "%s",
            "destinationVersion": 1,
            "eventType": "task15.execution",
            "payload": {"jobId": "%s"},
            "headers": {},
            "concurrencyPolicy": "ALLOW",
            "recoveryPolicy": "REQUEST_RECOVERY",
            "durable": true
          },
          "triggers": [
            {
              "triggerId": "%s",
              "jobId": "%s",
              "namespace": "%s",
              "name": "process-kill-trigger-%s",
              "description": "Task 15 recovery trigger",
              "spec": {
                "type": "ONCE",
                "fireAt": "%s"
              },
              "startAt": "%s",
              "endAt": null,
              "priority": 5,
              "timezone": "UTC",
              "misfirePolicy": "FIRE_NOW",
              "calendarNames": []
            }
          ]
        }
        """
            .formatted(
                jobId,
                NAMESPACE,
                jobId,
                destinationId,
                jobId,
                triggerId,
                jobId,
                NAMESPACE,
                triggerId,
                fireAt,
                fireAt);

    URI endpoint =
        URI.create(
            "http://%s:%d/api/v1/schedules".formatted(node.getHost(), node.getMappedPort(8080)));
    HttpRequest request =
        HttpRequest.newBuilder(endpoint)
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode())
        .as("create recovery-enabled schedule response: %s", response.body())
        .isEqualTo(201);
  }

  private static String awaitAcquiredNode(PostgreSQLContainer<?> postgres) {
    String[] acquiredBy = new String[1];
    await("a scheduler node acquires and blocks the firing")
        .pollInterval(Duration.ofMillis(100))
        .atMost(Duration.ofSeconds(25))
        .untilAsserted(
            () -> {
              Optional<String> marker = acquiredNode(postgres);
              assertThat(marker).isPresent();
              acquiredBy[0] = marker.orElseThrow();
            });
    return acquiredBy[0];
  }

  private static Optional<String> acquiredNode(PostgreSQLContainer<?> postgres)
      throws SQLException {
    try (Connection connection = connection(postgres);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                """
                select instance_id
                from scheduler.process_kill_acquired_marker
                order by acquired_at
                limit 1
                """)) {
      return result.next() ? Optional.ofNullable(result.getString(1)) : Optional.empty();
    }
  }

  private static void createAcquisitionMarkerTable(PostgreSQLContainer<?> postgres)
      throws SQLException {
    try (Connection connection = connection(postgres);
        Statement statement = connection.createStatement()) {
      statement.execute(
          """
          create table if not exists scheduler.process_kill_acquired_marker (
              fire_instance_id varchar(200) primary key,
              instance_id varchar(200) not null,
              trigger_name varchar(200) not null,
              acquired_at timestamptz not null default now()
          )
          """);
    }
  }

  private static void registerDestination(
      PostgreSQLContainer<?> postgres, UUID destinationId, String topic) throws SQLException {
    String sql =
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, ?, 'KAFKA', ?, 'execution:${executionId}', '{}'::jsonb,
                true, ?, 'task-15-test', ?, 'task-15-test')
        """;
    OffsetDateTime now = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    try (Connection connection = connection(postgres);
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setObject(1, destinationId);
      statement.setString(2, NAMESPACE);
      statement.setString(3, topic);
      statement.setObject(4, now);
      statement.setObject(5, now);
      statement.executeUpdate();
    }
  }

  private static int executionCount(PostgreSQLContainer<?> postgres, UUID executionId)
      throws SQLException {
    try (Connection connection = connection(postgres);
        PreparedStatement statement =
            connection.prepareStatement(
                "select count(*) from scheduler.execution where execution_id = ?")) {
      statement.setObject(1, executionId);
      try (ResultSet result = statement.executeQuery()) {
        result.next();
        return result.getInt(1);
      }
    }
  }

  private static boolean executionRecoveryFlag(PostgreSQLContainer<?> postgres, UUID executionId)
      throws SQLException {
    try (Connection connection = connection(postgres);
        PreparedStatement statement =
            connection.prepareStatement(
                "select coalesce((payload ->> 'recovery')::boolean, false) "
                    + "from scheduler.execution where execution_id = ?")) {
      statement.setObject(1, executionId);
      try (ResultSet result = statement.executeQuery()) {
        return result.next() && result.getBoolean(1);
      }
    }
  }

  private static int duplicateLogicalExecutionCount(PostgreSQLContainer<?> postgres)
      throws SQLException {
    try (Connection connection = connection(postgres);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                """
                select count(*)
                from (
                  select trigger_id, scheduled_fire_time
                  from scheduler.execution
                  where manual_fire_id is null
                  group by trigger_id, scheduled_fire_time
                  having count(*) > 1
                ) duplicates
                """)) {
      result.next();
      return result.getInt(1);
    }
  }

  private static void createTopic(KafkaContainer kafka, String topic) throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin
          .createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    }
  }

  private static Consumer<String, String> executionConsumer(KafkaContainer kafka, String topic) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "task15-events-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    Consumer<String, String> consumer =
        new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  private static void drainExecutionIds(
      Consumer<String, String> consumer, JsonMapper jsonMapper, List<UUID> executionIds) {
    consumer
        .poll(Duration.ofMillis(250))
        .forEach(
            record -> {
              try {
                JsonNode payload = jsonMapper.readTree(record.value());
                String executionId = payload.path("executionId").asText();
                if (!executionId.isBlank()) {
                  executionIds.add(UUID.fromString(executionId));
                }
              } catch (Exception exception) {
                throw new IllegalStateException("Invalid scheduler execution event", exception);
              }
            });
  }

  private static Connection connection(PostgreSQLContainer<?> postgres) throws SQLException {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static Path repositoryRoot() {
    Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    while (current != null) {
      if (Files.isRegularFile(current.resolve("pom.xml"))
          && Files.isRegularFile(current.resolve("scheduler-service/pom.xml"))) {
        return current;
      }
      current = current.getParent();
    }
    throw new IllegalStateException("Cannot locate mcs-scheduler repository root");
  }

  private static Path serviceJar(Path repositoryRoot) throws IOException {
    Path target = repositoryRoot.resolve("scheduler-service/target");
    if (!Files.isDirectory(target)) {
      throw new IllegalStateException("scheduler-service must be packaged before recovery IT runs");
    }
    try (var candidates = Files.list(target)) {
      return candidates
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith("scheduler-service-"))
          .filter(path -> path.getFileName().toString().endsWith(".jar"))
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "Cannot locate packaged scheduler-service executable jar in " + target));
    }
  }
}
