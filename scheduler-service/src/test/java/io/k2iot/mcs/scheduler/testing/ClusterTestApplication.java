package io.k2iot.mcs.scheduler.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.k2iot.mcs.scheduler.SchedulerApplication;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ClusterTestApplication implements AutoCloseable {

  private static final Duration CLUSTER_CHECK_IN = Duration.ofSeconds(1);
  private static final Duration MISFIRE_THRESHOLD = Duration.ofSeconds(3);
  private static final Duration SCHEDULE_WINDOW = Duration.ofSeconds(16);
  private static final Duration MAX_WAIT =
      SCHEDULE_WINDOW.plus(MISFIRE_THRESHOLD).plus(CLUSTER_CHECK_IN.multipliedBy(3)).plusSeconds(5);

  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("scheduler")
          .withUsername("scheduler")
          .withPassword("scheduler");

  private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");
  private static final JdbcTemplate JDBC;

  static {
    POSTGRES.start();
    KAFKA.start();

    Flyway.configure()
        .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
        .load()
        .migrate();

    var dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(POSTGRES.getDriverClassName());
    dataSource.setUrl(POSTGRES.getJdbcUrl());
    dataSource.setUsername(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    JDBC = new JdbcTemplate(dataSource);
  }

  private final String eventTopic = "mcs.scheduler.task14.executions." + UUID.randomUUID();

  private ClusterTestApplication() {
    createEventTopic();
  }

  public static ClusterTestApplication create() {
    return new ClusterTestApplication();
  }

  public void resetState() {
    JDBC.execute(
        """
        truncate table
          scheduler.audit_event,
          scheduler.outbox_event,
          scheduler.execution,
          scheduler.inbox_message,
          scheduler.command_request,
          scheduler.trigger_definition,
          scheduler.job_definition,
          scheduler.destination,
          quartz.qrtz_fired_triggers,
          quartz.qrtz_paused_trigger_grps,
          quartz.qrtz_scheduler_state,
          quartz.qrtz_locks,
          quartz.qrtz_simple_triggers,
          quartz.qrtz_simprop_triggers,
          quartz.qrtz_cron_triggers,
          quartz.qrtz_blob_triggers,
          quartz.qrtz_triggers,
          quartz.qrtz_job_details,
          quartz.qrtz_calendars
        restart identity cascade
        """);
  }

  public Node startNode(String nodeName) {
    String nodeId = nodeName + "-" + UUID.randomUUID();
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(SchedulerApplication.class)
            .run(
                "--spring.application.name=mcs-scheduler-" + nodeId,
                "--spring.main.banner-mode=off",
                "--server.port=0",
                "--spring.grpc.server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.datasource.driver-class-name=" + POSTGRES.getDriverClassName(),
                "--spring.flyway.enabled=true",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.quartz.properties.org.quartz.scheduler.instanceName=mcs-scheduler-task14",
                "--spring.quartz.properties.org.quartz.scheduler.instanceId=AUTO",
                "--spring.quartz.properties.org.quartz.jobStore.clusterCheckinInterval="
                    + CLUSTER_CHECK_IN.toMillis(),
                "--spring.quartz.properties.org.quartz.jobStore.misfireThreshold="
                    + MISFIRE_THRESHOLD.toMillis(),
                "--spring.quartz.properties.org.quartz.threadPool.threadCount=4",
                "--mcs.scheduler.instance-id=" + nodeId,
                "--mcs.scheduler.kafka.command-topic=mcs.scheduler.task14.commands",
                "--mcs.scheduler.kafka.command-result-topic=mcs.scheduler.task14.command-results",
                "--mcs.scheduler.kafka.dlt-topic=mcs.scheduler.task14.commands.DLT",
                "--mcs.scheduler.kafka.consumer-group=mcs-scheduler-task14",
                "--mcs.scheduler.kafka.partitions=1",
                "--mcs.scheduler.kafka.replicas=1",
                "--mcs.scheduler.kafka.retry-attempts=0",
                "--mcs.scheduler.outbox.enabled=true",
                "--mcs.scheduler.outbox.poll-interval=100ms",
                "--mcs.scheduler.outbox.claim-timeout=2s",
                "--mcs.scheduler.outbox.publish-timeout=5s");

    try {
      String quartzInstanceId = context.getBean(Scheduler.class).getSchedulerInstanceId();
      return new Node(context, quartzInstanceId);
    } catch (SchedulerException exception) {
      context.close();
      throw new IllegalStateException("Failed to resolve Quartz instance ID", exception);
    }
  }

  public void registerKafkaDestination(Node node, UUID destinationId, String namespace) {
    OffsetDateTime now = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    node.jdbc()
        .update(
            """
            insert into scheduler.destination (
                destination_id, version, namespace, type, topic, key_expression, headers,
                enabled, created_at, created_by, updated_at, updated_by)
            values (?, 1, ?, 'KAFKA', ?, 'execution:${executionId}', '{}'::jsonb,
                    true, ?, 'task-14-test', ?, 'task-14-test')
            """,
            destinationId,
            namespace,
            eventTopic,
            now,
            now);
  }

  public void createOneShotSchedule(
      Node node,
      UUID jobId,
      UUID triggerId,
      UUID destinationId,
      String namespace,
      Instant fireAt) {
    var job =
        new SchedulerCommands.JobDraft(
            jobId,
            namespace,
            "task14-job-" + jobId,
            "Task 14 cluster execution",
            destinationId,
            1,
            "task14.execution",
            Map.of("jobId", jobId.toString(), "triggerId", triggerId.toString()),
            Map.of(),
            ConcurrencyPolicy.ALLOW,
            RecoveryPolicy.NONE,
            true);
    var trigger =
        new SchedulerCommands.TriggerDraft(
            triggerId,
            jobId,
            namespace,
            "task14-trigger-" + triggerId,
            "Task 14 one-shot trigger",
            new OnceTriggerSpec(fireAt),
            fireAt,
            null,
            5,
            "UTC",
            TriggerDefinition.MisfirePolicy.FIRE_NOW,
            Set.of());

    node.commands()
        .createSchedule(
            new SchedulerCommands.CreateSchedule(
                UUID.randomUUID(), job, List.of(trigger), "task-14-test"));
  }

  public Consumer<String, String> newExecutionConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "task14-events-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    Consumer<String, String> consumer =
        new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(List.of(eventTopic));
    return consumer;
  }

  public Set<UUID> awaitKafkaExecutionIds(
      Consumer<String, String> consumer, Node node, int expectedCount) {
    Set<UUID> executionIds = new LinkedHashSet<>();
    await("Kafka execution IDs from Quartz node " + node.quartzInstanceId())
        .atMost(MAX_WAIT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> {
              consumer
                  .poll(Duration.ofMillis(250))
                  .forEach(
                      record -> {
                        JsonNode payload = readPayload(node.jsonMapper(), record.value());
                        String executionId = payload.path("executionId").asText();
                        if (!executionId.isBlank()) {
                          executionIds.add(UUID.fromString(executionId));
                        }
                      });
              assertThat(executionIds).hasSize(expectedCount);
            });
    return Set.copyOf(executionIds);
  }

  public int executionCount(Node node) {
    return node.jdbc()
        .queryForObject("select count(*) from scheduler.execution", Integer.class);
  }

  public int executionCount(Node node, UUID executionId) {
    return node.jdbc()
        .queryForObject(
            "select count(*) from scheduler.execution where execution_id = ?",
            Integer.class,
            executionId);
  }

  public int distinctExecutionCount(Node node) {
    return node.jdbc()
        .queryForObject(
            "select count(distinct execution_id) from scheduler.execution", Integer.class);
  }

  public int duplicateLogicalExecutionCount(Node node) {
    return node.jdbc()
        .queryForObject(
            """
            select count(*)
            from (
              select trigger_id, scheduled_fire_time
              from scheduler.execution
              where manual_fire_id is null
              group by trigger_id, scheduled_fire_time
              having count(*) > 1
            ) duplicates
            """,
            Integer.class);
  }

  private void createEventTopic() {
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(eventTopic, 3, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to create Kafka event topic " + eventTopic, exception);
    }
  }

  private JsonNode readPayload(JsonMapper jsonMapper, String payload) {
    try {
      return jsonMapper.readTree(payload);
    } catch (Exception exception) {
      throw new IllegalStateException("Invalid scheduler execution event payload", exception);
    }
  }

  @Override
  public void close() {
    // Shared Testcontainers live for the test JVM; individual nodes and consumers own their lifecycle.
  }

  public record Node(ConfigurableApplicationContext context, String quartzInstanceId)
      implements AutoCloseable {

    public SchedulerCommandFacade commands() {
      return context.getBean(SchedulerCommandFacade.class);
    }

    public JdbcTemplate jdbc() {
      return context.getBean(JdbcTemplate.class);
    }

    public JsonMapper jsonMapper() {
      return context.getBean(JsonMapper.class);
    }

    @Override
    public void close() {
      context.close();
    }
  }
}
