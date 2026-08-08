package io.k2iot.mcs.scheduler.acceptance;

import static io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy.CONCURRENCY_POLICY_ALLOW;
import static io.k2iot.mcs.scheduler.v1.RecoveryPolicy.RECOVERY_POLICY_NONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.k2iot.mcs.scheduler.v1.CreateJobRequest;
import io.k2iot.mcs.scheduler.v1.GetJobRequest;
import io.k2iot.mcs.scheduler.v1.JobDraft;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ListJobTriggersRequest;
import io.k2iot.mcs.scheduler.v1.SchedulerCommandServiceGrpc;
import io.k2iot.mcs.scheduler.v1.SchedulerQueryServiceGrpc;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class FinalAcceptanceIT {

  private static final String NAMESPACE = "final-acceptance";
  private static final String COMMAND_TOPIC = "mcs.scheduler.final.commands";
  private static final String COMMAND_RESULT_TOPIC = "mcs.scheduler.final.command-results";
  private static final String DLT_TOPIC = "mcs.scheduler.final.commands.DLT";
  private static final String REST_EVENT_TOPIC = "mcs.scheduler.final.rest-events";
  private static final String GRPC_EVENT_TOPIC = "mcs.scheduler.final.grpc-events";
  private static final String KAFKA_EVENT_TOPIC = "mcs.scheduler.final.kafka-events";
  private static final String SECRET_MARKER = "final-acceptance-secret-must-not-leak";
  private static final JsonMapper JSON = JsonMapper.builder().findAndAddModules().build();

  @Test
  void verifiesFinalCrossInterfaceAcceptanceAndIdempotency() throws Exception {
    Path repositoryRoot = repositoryRoot();
    Path testDockerfile =
        repositoryRoot.resolve("scheduler-service/src/test/resources/cluster/Dockerfile.test");
    Path serviceJar = serviceJar(repositoryRoot);

    assertThat(testDockerfile).isRegularFile();
    assertThat(serviceJar).isRegularFile();

    ImageFromDockerfile schedulerImage =
        new ImageFromDockerfile("mcs-scheduler-final-acceptance:" + UUID.randomUUID(), true)
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
        GenericContainer<?> node = schedulerNode(schedulerImage, network, postgres, kafka)) {
      postgres.start();
      kafka.start();
      createTopic(kafka, COMMAND_TOPIC);
      createTopic(kafka, COMMAND_RESULT_TOPIC);
      createTopic(kafka, DLT_TOPIC);
      createTopic(kafka, REST_EVENT_TOPIC);
      createTopic(kafka, GRPC_EVENT_TOPIC);
      createTopic(kafka, KAFKA_EVENT_TOPIC);
      node.start();

      UUID restDestinationId = UUID.randomUUID();
      UUID grpcDestinationId = UUID.randomUUID();
      UUID kafkaDestinationId = UUID.randomUUID();
      registerDestination(postgres, restDestinationId, REST_EVENT_TOPIC);
      registerDestination(postgres, grpcDestinationId, GRPC_EVENT_TOPIC);
      registerDestination(postgres, kafkaDestinationId, KAFKA_EVENT_TOPIC);

      ManagedChannel channel =
          ManagedChannelBuilder.forAddress(node.getHost(), node.getMappedPort(9090))
              .usePlaintext()
              .build();
      try (Consumer<String, String> restEvents = consumer(kafka, REST_EVENT_TOPIC);
          Consumer<String, String> commandResults = consumer(kafka, COMMAND_RESULT_TOPIC);
          Consumer<String, String> kafkaEvents = consumer(kafka, KAFKA_EVENT_TOPIC);
          Consumer<String, String> dlt = consumer(kafka, DLT_TOPIC);
          KafkaProducer<String, String> producer = producer(kafka)) {
        verifyRestCreationReplayConflictAndGrpcRead(
            node, channel, postgres, restDestinationId, restEvents);
        verifyGrpcReplayAndConflict(channel, postgres, grpcDestinationId);
        verifyKafkaCreationReplayConflictAndExecution(
            kafka, producer, commandResults, kafkaEvents, dlt, postgres, kafkaDestinationId);
      } finally {
        channel.shutdownNow();
        assertThat(channel.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }

      assertThat(node.getLogs()).doesNotContain(SECRET_MARKER);
    }
  }

  private static void verifyRestCreationReplayConflictAndGrpcRead(
      GenericContainer<?> node,
      ManagedChannel channel,
      PostgreSQLContainer<?> postgres,
      UUID destinationId,
      Consumer<String, String> restEvents)
      throws Exception {
    UUID requestId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();
    UUID firstTriggerId = UUID.randomUUID();
    UUID secondTriggerId = UUID.randomUUID();
    Instant firstFireAt = Instant.now().plusSeconds(8).truncatedTo(ChronoUnit.MILLIS);
    Instant secondFireAt = Instant.now().plus(Duration.ofHours(1)).truncatedTo(ChronoUnit.MILLIS);
    String body =
        restScheduleBody(
            jobId,
            firstTriggerId,
            secondTriggerId,
            destinationId,
            "rest-two-trigger-job",
            Map.of("source", "rest"),
            firstFireAt,
            secondFireAt);

    HttpResponse<String> created = postSchedule(node, requestId, body);
    HttpResponse<String> replayed = postSchedule(node, requestId, body);

    assertThat(created.statusCode()).isEqualTo(201);
    assertThat(replayed.statusCode()).isEqualTo(201);
    assertThat(jobCount(postgres, jobId)).isEqualTo(1);
    assertThat(triggerCount(postgres, jobId)).isEqualTo(2);

    var query =
        SchedulerQueryServiceGrpc.newBlockingStub(channel)
            .withWaitForReady()
            .withDeadlineAfter(15, TimeUnit.SECONDS);
    JobResponse grpcJob =
        query.getJob(
            GetJobRequest.newBuilder().setNamespace(NAMESPACE).setJobId(jobId.toString()).build());
    var grpcTriggers =
        query.listJobTriggers(
            ListJobTriggersRequest.newBuilder()
                .setNamespace(NAMESPACE)
                .setJobId(jobId.toString())
                .setPageSize(10)
                .build());

    assertThat(grpcJob.getJob().getJobId()).isEqualTo(jobId.toString());
    assertThat(grpcJob.getJob().getName()).isEqualTo("rest-two-trigger-job");
    assertThat(grpcTriggers.getTriggersCount()).isEqualTo(2);

    String conflictingBody =
        restScheduleBody(
            jobId,
            firstTriggerId,
            secondTriggerId,
            destinationId,
            "rest-two-trigger-job",
            Map.of("source", "rest", "secret", SECRET_MARKER),
            firstFireAt,
            secondFireAt);
    HttpResponse<String> conflict = postSchedule(node, requestId, conflictingBody);
    assertThat(conflict.statusCode()).isEqualTo(409);
    assertThat(conflict.body()).contains("IDEMPOTENCY_CONFLICT").doesNotContain(SECRET_MARKER);

    UUID expectedExecutionId =
        io.k2iot.mcs.scheduler.execution.ExecutionIdentity.forScheduled(
            firstTriggerId, firstFireAt);
    List<UUID> observed = new ArrayList<>();
    await("REST-created schedule publishes its execution event")
        .pollInSameThread()
        .pollInterval(Duration.ofMillis(250))
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              drainExecutionIds(restEvents, observed);
              assertThat(observed).contains(expectedExecutionId);
            });
    await("REST-created execution remains unique")
        .pollInSameThread()
        .pollInterval(Duration.ofMillis(250))
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              drainExecutionIds(restEvents, observed);
              assertThat(observed.stream().filter(expectedExecutionId::equals).count())
                  .isEqualTo(1);
            });
  }

  private static void verifyGrpcReplayAndConflict(
      ManagedChannel channel, PostgreSQLContainer<?> postgres, UUID destinationId) {
    UUID requestId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();
    var command =
        SchedulerCommandServiceGrpc.newBlockingStub(channel)
            .withWaitForReady()
            .withDeadlineAfter(15, TimeUnit.SECONDS);
    CreateJobRequest request =
        grpcCreateJobRequest(requestId, jobId, destinationId, "grpc-job", "grpc");

    JobResponse created = command.createJob(request);
    JobResponse replayed = command.createJob(request);

    assertThat(replayed).isEqualTo(created);
    assertThat(jobCountUnchecked(postgres, jobId)).isEqualTo(1);

    CreateJobRequest conflicting =
        request.toBuilder()
            .setJob(
                request.getJob().toBuilder()
                    .setDescription("conflicting description " + SECRET_MARKER)
                    .build())
            .build();
    StatusRuntimeException conflict =
        catchThrowableOfType(() -> command.createJob(conflicting), StatusRuntimeException.class);

    assertThat(conflict.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(conflict.getStatus().getDescription()).doesNotContain(SECRET_MARKER);
    Metadata trailers = conflict.getTrailers();
    assertThat(trailers).isNotNull();
    assertThat(
            trailers.get(Metadata.Key.of("scheduler-error-code", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("IDEMPOTENCY_CONFLICT");
  }

  private static void verifyKafkaCreationReplayConflictAndExecution(
      KafkaContainer kafka,
      KafkaProducer<String, String> producer,
      Consumer<String, String> commandResults,
      Consumer<String, String> kafkaEvents,
      Consumer<String, String> dlt,
      PostgreSQLContainer<?> postgres,
      UUID destinationId)
      throws Exception {
    UUID requestId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();
    UUID triggerId = UUID.randomUUID();
    Instant fireAt = Instant.now().plusSeconds(8).truncatedTo(ChronoUnit.MILLIS);
    String payload = kafkaSchedulePayload(jobId, triggerId, destinationId, "kafka-job", fireAt);
    String key = NAMESPACE + ":" + jobId;

    sendKafkaCommand(producer, key, UUID.randomUUID(), requestId, payload);
    sendKafkaCommand(producer, key, UUID.randomUUID(), requestId, payload);

    List<JsonNode> results = new ArrayList<>();
    await("Kafka command replay emits stored command results")
        .pollInSameThread()
        .pollInterval(Duration.ofMillis(250))
        .atMost(Duration.ofSeconds(25))
        .untilAsserted(
            () -> {
              drainCommandResults(commandResults, requestId, results);
              assertThat(results).hasSize(2);
              assertThat(results)
                  .allSatisfy(
                      result -> assertThat(result.path("status").asText()).isEqualTo("SUCCEEDED"));
            });

    assertThat(jobCount(postgres, jobId)).isEqualTo(1);
    assertThat(triggerCount(postgres, jobId)).isEqualTo(1);

    UUID expectedExecutionId =
        io.k2iot.mcs.scheduler.execution.ExecutionIdentity.forScheduled(triggerId, fireAt);
    List<UUID> observedExecutions = new ArrayList<>();
    await("Kafka-created schedule publishes registered destination event")
        .pollInSameThread()
        .pollInterval(Duration.ofMillis(250))
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              drainExecutionIds(kafkaEvents, observedExecutions);
              assertThat(observedExecutions).containsExactly(expectedExecutionId);
            });

    String conflictingPayload =
        kafkaSchedulePayload(jobId, triggerId, destinationId, "kafka-job-" + SECRET_MARKER, fireAt);
    sendKafkaCommand(producer, key, UUID.randomUUID(), requestId, conflictingPayload);

    await("Kafka request-id conflict reaches DLT with stable error code")
        .pollInSameThread()
        .pollInterval(Duration.ofMillis(250))
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> assertThat(readDltErrorCodes(dlt)).contains("IDEMPOTENCY_CONFLICT"));
    assertThat(jobCount(postgres, jobId)).isEqualTo(1);
  }

  private static GenericContainer<?> schedulerNode(
      ImageFromDockerfile image,
      Network network,
      PostgreSQLContainer<?> postgres,
      KafkaContainer kafka) {
    return new GenericContainer<>(image)
        .withNetwork(network)
        .withExposedPorts(8080, 9090)
        .dependsOn(postgres, kafka)
        .withEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/scheduler")
        .withEnv("SPRING_DATASOURCE_USERNAME", "scheduler")
        .withEnv("SPRING_DATASOURCE_PASSWORD", "scheduler")
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        .withEnv("MCS_SCHEDULER_INSTANCE_ID", "final-acceptance-node")
        .withEnv("MCS_SCHEDULER_KAFKA_COMMAND_TOPIC", COMMAND_TOPIC)
        .withEnv("MCS_SCHEDULER_KAFKA_COMMAND_RESULT_TOPIC", COMMAND_RESULT_TOPIC)
        .withEnv("MCS_SCHEDULER_KAFKA_DLT_TOPIC", DLT_TOPIC)
        .withEnv("MCS_SCHEDULER_KAFKA_CONSUMER_GROUP", "mcs-scheduler-final-acceptance")
        .withEnv("MCS_SCHEDULER_KAFKA_PARTITIONS", "1")
        .withEnv("MCS_SCHEDULER_KAFKA_REPLICAS", "1")
        .withEnv("MCS_SCHEDULER_KAFKA_RETRY_ATTEMPTS", "0")
        .withEnv("MCS_SCHEDULER_OUTBOX_ENABLED", "true")
        .withEnv("MCS_SCHEDULER_OUTBOX_POLL_INTERVAL", "100ms")
        .withEnv("MCS_SCHEDULER_OUTBOX_CLAIM_TIMEOUT", "2s")
        .withEnv("MCS_SCHEDULER_OUTBOX_PUBLISH_TIMEOUT", "5s")
        .waitingFor(
            Wait.forHttp("/actuator/health")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private static HttpResponse<String> postSchedule(
      GenericContainer<?> node, UUID requestId, String body)
      throws IOException, InterruptedException {
    URI endpoint =
        URI.create(
            "http://%s:%d/api/v1/schedules".formatted(node.getHost(), node.getMappedPort(8080)));
    HttpRequest request =
        HttpRequest.newBuilder(endpoint)
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", requestId.toString())
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static String restScheduleBody(
      UUID jobId,
      UUID firstTriggerId,
      UUID secondTriggerId,
      UUID destinationId,
      String jobName,
      Map<String, String> payload,
      Instant firstFireAt,
      Instant secondFireAt)
      throws Exception {
    String payloadJson = JSON.writeValueAsString(payload);
    return """
        {
          "caller":"final-rest",
          "job":{
            "jobId":"%s",
            "namespace":"%s",
            "name":"%s",
            "description":"Final REST acceptance",
            "destinationId":"%s",
            "destinationVersion":1,
            "eventType":"final.rest.execution",
            "payload":%s,
            "headers":{},
            "concurrencyPolicy":"ALLOW",
            "recoveryPolicy":"NONE",
            "durable":true
          },
          "triggers":[
            {
              "triggerId":"%s",
              "jobId":"%s",
              "namespace":"%s",
              "name":"rest-first",
              "description":"First final REST trigger",
              "spec":{"type":"ONCE","fireAt":"%s"},
              "startAt":"%s",
              "endAt":null,
              "priority":5,
              "timezone":"UTC",
              "misfirePolicy":"FIRE_NOW",
              "calendarNames":[]
            },
            {
              "triggerId":"%s",
              "jobId":"%s",
              "namespace":"%s",
              "name":"rest-second",
              "description":"Second final REST trigger",
              "spec":{"type":"ONCE","fireAt":"%s"},
              "startAt":"%s",
              "endAt":null,
              "priority":5,
              "timezone":"UTC",
              "misfirePolicy":"FIRE_NOW",
              "calendarNames":[]
            }
          ]
        }
        """
        .formatted(
            jobId,
            NAMESPACE,
            jobName,
            destinationId,
            payloadJson,
            firstTriggerId,
            jobId,
            NAMESPACE,
            firstFireAt,
            firstFireAt,
            secondTriggerId,
            jobId,
            NAMESPACE,
            secondFireAt,
            secondFireAt);
  }

  private static CreateJobRequest grpcCreateJobRequest(
      UUID requestId, UUID jobId, UUID destinationId, String name, String source) {
    Struct payload =
        Struct.newBuilder()
            .putFields("source", Value.newBuilder().setStringValue(source).build())
            .build();
    return CreateJobRequest.newBuilder()
        .setRequestId(requestId.toString())
        .setNamespace(NAMESPACE)
        .setCaller("final-grpc")
        .setJob(
            JobDraft.newBuilder()
                .setJobId(jobId.toString())
                .setName(name)
                .setDescription("Final gRPC idempotency acceptance")
                .setDestinationId(destinationId.toString())
                .setDestinationVersion(1)
                .setEventType("final.grpc.execution")
                .setPayload(payload)
                .setConcurrencyPolicy(CONCURRENCY_POLICY_ALLOW)
                .setRecoveryPolicy(RECOVERY_POLICY_NONE)
                .setDurable(true)
                .build())
        .build();
  }

  private static String kafkaSchedulePayload(
      UUID jobId, UUID triggerId, UUID destinationId, String name, Instant fireAt)
      throws Exception {
    return """
        {
          "job":{
            "jobId":"%s",
            "namespace":"%s",
            "name":"%s",
            "description":"Final Kafka acceptance",
            "destinationId":"%s",
            "destinationVersion":1,
            "eventType":"final.kafka.execution",
            "payload":{"source":"kafka"},
            "headers":{},
            "concurrencyPolicy":"ALLOW",
            "recoveryPolicy":"NONE",
            "durable":true
          },
          "triggers":[{
            "triggerId":"%s",
            "jobId":"%s",
            "namespace":"%s",
            "name":"kafka-once",
            "description":"Final Kafka trigger",
            "spec":{"type":"ONCE","fireAt":"%s"},
            "startAt":"%s",
            "endAt":null,
            "priority":5,
            "timezone":"UTC",
            "misfirePolicy":"FIRE_NOW",
            "calendarNames":[]
          }]
        }
        """
        .formatted(
            jobId, NAMESPACE, name, destinationId, triggerId, jobId, NAMESPACE, fireAt, fireAt);
  }

  private static void sendKafkaCommand(
      KafkaProducer<String, String> producer,
      String key,
      UUID messageId,
      UUID requestId,
      String payload)
      throws Exception {
    String envelope =
        """
        {
          "schemaVersion":1,
          "messageId":"%s",
          "requestId":"%s",
          "occurredAt":"%s",
          "producer":"final-kafka",
          "namespace":"%s",
          "commandType":"CREATE_SCHEDULE",
          "payload":%s
        }
        """
            .formatted(messageId, requestId, Instant.now(), NAMESPACE, payload);
    producer.send(new ProducerRecord<>(COMMAND_TOPIC, key, envelope)).get(10, TimeUnit.SECONDS);
  }

  private static KafkaProducer<String, String> producer(KafkaContainer kafka) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
  }

  private static Consumer<String, String> consumer(KafkaContainer kafka, String topic) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "final-acceptance-" + topic + "-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    Consumer<String, String> consumer =
        new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  private static void drainCommandResults(
      Consumer<String, String> consumer, UUID requestId, List<JsonNode> results) {
    consumer
        .poll(Duration.ofMillis(250))
        .forEach(
            record -> {
              try {
                JsonNode result = JSON.readTree(record.value());
                if (requestId.toString().equals(result.path("requestId").asText())) {
                  results.add(result);
                }
              } catch (Exception exception) {
                throw new IllegalStateException("Invalid command result", exception);
              }
            });
  }

  private static void drainExecutionIds(
      Consumer<String, String> consumer, List<UUID> executionIds) {
    consumer
        .poll(Duration.ofMillis(250))
        .forEach(
            record -> {
              try {
                JsonNode payload = JSON.readTree(record.value());
                String executionId = payload.path("executionId").asText();
                if (!executionId.isBlank()) {
                  executionIds.add(UUID.fromString(executionId));
                }
              } catch (Exception exception) {
                throw new IllegalStateException("Invalid scheduler execution event", exception);
              }
            });
  }

  private static List<String> readDltErrorCodes(Consumer<String, String> consumer) {
    List<String> codes = new ArrayList<>();
    consumer
        .poll(Duration.ofMillis(250))
        .forEach(
            record -> {
              Header header = record.headers().lastHeader("mcs.scheduler.errorCode");
              if (header != null && header.value() != null) {
                codes.add(new String(header.value(), StandardCharsets.UTF_8));
              }
            });
    return codes;
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

  private static void registerDestination(
      PostgreSQLContainer<?> postgres, UUID destinationId, String topic) throws SQLException {
    String sql =
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, ?, 'KAFKA', ?, 'execution:${executionId}', '{}'::jsonb,
                true, ?, 'final-acceptance', ?, 'final-acceptance')
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

  private static int jobCount(PostgreSQLContainer<?> postgres, UUID jobId) throws SQLException {
    return count(postgres, "select count(*) from scheduler.job_definition where job_id = ?", jobId);
  }

  private static int jobCountUnchecked(PostgreSQLContainer<?> postgres, UUID jobId) {
    try {
      return jobCount(postgres, jobId);
    } catch (SQLException exception) {
      throw new IllegalStateException(exception);
    }
  }

  private static int triggerCount(PostgreSQLContainer<?> postgres, UUID jobId) throws SQLException {
    return count(
        postgres, "select count(*) from scheduler.trigger_definition where job_id = ?", jobId);
  }

  private static int count(PostgreSQLContainer<?> postgres, String sql, UUID id)
      throws SQLException {
    try (Connection connection = connection(postgres);
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setObject(1, id);
      try (ResultSet result = statement.executeQuery()) {
        result.next();
        return result.getInt(1);
      }
    }
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
      throw new IllegalStateException(
          "scheduler-service must be packaged before final acceptance IT runs");
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
