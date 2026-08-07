package io.k2iot.mcs.scheduler.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.k2iot.mcs.scheduler.SchedulerApplication;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(
    classes = SchedulerApplication.class,
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.quartz.autoconfigure.QuartzAutoConfiguration",
      "spring.flyway.enabled=true",
      "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "spring.kafka.consumer.auto-offset-reset=earliest",
      "mcs.scheduler.instance-id=task-11-it",
      "mcs.scheduler.outbox.enabled=false"
    })
@EmbeddedKafka(partitions = 1, topics = OutboxPublisherKafkaIT.EVENT_TOPIC)
class OutboxPublisherKafkaIT {

  static final String EVENT_TOPIC = "billing.scheduler.executions.test.v1";

  private static final Instant NOW = Instant.parse("2026-08-07T13:00:00Z");
  private static final UUID DESTINATION_ID =
      UUID.fromString("81000000-0000-4000-8000-000000000001");
  private static final UUID JOB_ID = UUID.fromString("82000000-0000-4000-8000-000000000001");
  private static final UUID TRIGGER_ID = UUID.fromString("83000000-0000-4000-8000-000000000001");
  private static final UUID EXECUTION_ID = UUID.fromString("84000000-0000-4000-8000-000000000001");

  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("scheduler")
          .withUsername("scheduler")
          .withPassword("scheduler");

  static {
    POSTGRES.start();
  }

  @DynamicPropertySource
  static void registerPostgresProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
  }

  @Autowired JdbcTemplate jdbc;
  @Autowired OutboxPublisher publisher;
  @Autowired EmbeddedKafkaBroker embeddedKafka;
  @Autowired ProducerFactory<Object, Object> producerFactory;
  @Autowired JsonMapper jsonMapper;

  @BeforeEach
  void setUp() {
    cleanState();
    insertExecutionOutbox();
  }

  @AfterEach
  void tearDown() {
    cleanState();
  }

  @Test
  void publishesExecutionEventThenMarksOutboxAndExecutionDelivered() throws Exception {
    try (Consumer<String, String> consumer = eventConsumer()) {
      embeddedKafka.consumeFromAnEmbeddedTopic(consumer, EVENT_TOPIC);

      publisher.publishOnce();

      ConsumerRecord<String, String> record = awaitEvent(consumer, Duration.ofSeconds(10));
      JsonNode payload = jsonMapper.readTree(record.value());
      assertThat(record.key()).isEqualTo("job:" + JOB_ID);
      assertThat(payload.path("executionId").asText()).isEqualTo(EXECUTION_ID.toString());
      assertThat(
              jdbc.queryForObject(
                  "select state from scheduler.outbox_event where outbox_event_id = ?",
                  String.class,
                  EXECUTION_ID))
          .isEqualTo("PUBLISHED");
      assertThat(
              jdbc.queryForObject(
                  "select published_at is not null from scheduler.outbox_event where outbox_event_id = ?",
                  Boolean.class,
                  EXECUTION_ID))
          .isTrue();
      assertThat(
              jdbc.queryForObject(
                  "select status from scheduler.execution where execution_id = ?",
                  String.class,
                  EXECUTION_ID))
          .isEqualTo("DELIVERED");
    }
  }

  @Test
  void configuresDurableIdempotentKafkaProducer() {
    assertThat(producerFactory).isInstanceOf(DefaultKafkaProducerFactory.class);
    Map<String, Object> configuration =
        ((DefaultKafkaProducerFactory<?, ?>) producerFactory).getConfigurationProperties();

    assertThat(configuration.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG).toString())
        .isEqualTo("true");
    assertThat(configuration.get(ProducerConfig.ACKS_CONFIG).toString()).isEqualTo("all");
    assertThat(
            Integer.parseInt(
                configuration.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG).toString()))
        .isBetween(1, 120_000);
    assertThat(configuration.get(ProducerConfig.CLIENT_ID_CONFIG).toString())
        .contains("task-11-it");
  }

  private Consumer<String, String> eventConsumer() {
    Map<String, Object> consumerProperties =
        KafkaTestUtils.consumerProps(
            embeddedKafka, "task-11-event-reader-" + UUID.randomUUID(), false);
    return new DefaultKafkaConsumerFactory<>(
            consumerProperties, new StringDeserializer(), new StringDeserializer())
        .createConsumer();
  }

  private ConsumerRecord<String, String> awaitEvent(
      Consumer<String, String> consumer, Duration timeout) {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
        if (EVENT_TOPIC.equals(record.topic())) {
          return record;
        }
      }
    }
    fail("Timed out waiting for execution outbox event");
    throw new AssertionError("unreachable");
  }

  private void insertExecutionOutbox() {
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, 'billing', 'KAFKA', ?, 'job:${jobId}', '{}'::jsonb,
                true, ?, 'task-11-test', ?, 'task-11-test')
        """,
        DESTINATION_ID,
        EVENT_TOPIC,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
    jdbc.update(
        """
        insert into scheduler.job_definition (
            job_id, namespace, name, description, destination_id, destination_version,
            event_type, payload, headers, concurrency_policy, recovery_policy, durable,
            state, revision, created_at, created_by, updated_at, updated_by)
        values (?, 'billing', 'invoice-delivery', null, ?, 1,
                'billing.invoice.due', '{"invoiceId":"INV-11"}'::jsonb, '{}'::jsonb,
                'ALLOW', 'NONE', true, 'ACTIVE', 1, ?, 'task-11-test', ?, 'task-11-test')
        """,
        JOB_ID,
        DESTINATION_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
    jdbc.update(
        """
        insert into scheduler.trigger_definition (
            trigger_id, job_id, namespace, name, description, type, spec,
            start_at, end_at, priority, timezone, misfire_policy, calendar_names,
            state, revision, created_at, created_by, updated_at, updated_by)
        values (?, ?, 'billing', 'invoice-once', null, 'ONCE',
                '{"type":"ONCE","fireAt":"2030-01-01T00:00:00Z"}'::jsonb,
                ?, null, 5, null, 'FIRE_NOW', '[]'::jsonb,
                'ACTIVE', 1, ?, 'task-11-test', ?, 'task-11-test')
        """,
        TRIGGER_ID,
        JOB_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
    jdbc.update(
        """
        insert into scheduler.execution (
            execution_id, job_id, trigger_id, manual_fire_id, scheduled_fire_time,
            actual_fire_time, status, attempt, payload, created_at, updated_at)
        values (?, ?, ?, null, ?, ?, 'SCHEDULED', 1,
                '{"source":"task-11"}'::jsonb, ?, ?)
        """,
        EXECUTION_ID,
        JOB_ID,
        TRIGGER_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));

    String payload =
        """
        {
          "schemaVersion": 1,
          "executionId": "%s",
          "namespace": "billing",
          "eventType": "billing.invoice.due",
          "jobId": "%s",
          "triggerId": "%s",
          "scheduledFireTime": "%s",
          "actualFireTime": "%s",
          "recovery": false,
          "payload": {"invoiceId": "INV-11"},
          "headers": {}
        }
        """
            .formatted(EXECUTION_ID, JOB_ID, TRIGGER_ID, NOW, NOW);
    String headers =
        """
        {
          "topic": "%s",
          "key": "%s",
          "keyExpression": "job:${jobId}"
        }
        """
            .formatted(EVENT_TOPIC, EXECUTION_ID);
    jdbc.update(
        """
        insert into scheduler.outbox_event (
            outbox_event_id, aggregate_type, aggregate_id, event_type,
            payload, headers, occurred_at, published_at, publish_attempts, last_error)
        values (?, 'EXECUTION', ?, 'SCHEDULED_EXECUTION', cast(? as jsonb),
                cast(? as jsonb), ?, null, 0, null)
        """,
        EXECUTION_ID,
        EXECUTION_ID,
        payload,
        headers,
        NOW.atOffset(ZoneOffset.UTC));
  }

  private void cleanState() {
    jdbc.update("delete from scheduler.outbox_event");
    jdbc.update("delete from scheduler.execution");
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
  }
}
