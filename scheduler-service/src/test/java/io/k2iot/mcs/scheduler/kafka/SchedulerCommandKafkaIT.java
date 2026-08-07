package io.k2iot.mcs.scheduler.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.SchedulerApplication;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest(
    classes = SchedulerApplication.class,
    properties = {
      "spring.flyway.enabled=true",
      "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "spring.kafka.consumer.auto-offset-reset=earliest",
      "spring.kafka.consumer.enable-auto-commit=false",
      "mcs.scheduler.kafka.consumer-group=task-9-kafka-it"
    })
@EmbeddedKafka(
    partitions = 1,
    topics = {"mcs.scheduler.commands.v1", "mcs.scheduler.commands.v1.DLT"})
class SchedulerCommandKafkaIT {

  private static final String COMMAND_TOPIC = "mcs.scheduler.commands.v1";
  private static final String DLT_TOPIC = "mcs.scheduler.commands.v1.DLT";
  private static final Instant NOW = Instant.parse("2026-08-07T04:00:00Z");
  private static final UUID MESSAGE_ID = UUID.fromString("51000000-0000-4000-8000-000000000001");
  private static final UUID REQUEST_ID = UUID.fromString("52000000-0000-4000-8000-000000000001");
  private static final UUID JOB_ID = UUID.fromString("53000000-0000-4000-8000-000000000001");
  private static final UUID TRIGGER_ID = UUID.fromString("54000000-0000-4000-8000-000000000001");
  private static final UUID DESTINATION_ID =
      UUID.fromString("55000000-0000-4000-8000-000000000001");

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
  @Autowired KafkaTemplate<String, String> kafkaTemplate;
  @Autowired EmbeddedKafkaBroker embeddedKafka;
  @Autowired Scheduler scheduler;

  @BeforeEach
  void cleanState() throws Exception {
    scheduler.clear();
    jdbc.update("delete from scheduler.outbox_event");
    jdbc.update("delete from scheduler.inbox_message");
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
    jdbc.update("delete from scheduler.command_request");
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, 'billing', 'KAFKA', 'billing.invoice.commands', '${jobId}',
                '{}'::jsonb, true, ?, 'task-9-test', ?, 'task-9-test')
        """,
        DESTINATION_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
  }

  @Test
  void duplicateKafkaCommandCreatesOneInboxJobAndTrigger() throws Exception {
    String command = createScheduleEnvelope();
    String key = "billing:" + JOB_ID;

    kafkaTemplate.send(COMMAND_TOPIC, key, command).get();
    kafkaTemplate.send(COMMAND_TOPIC, key, command).get();

    awaitCount("scheduler.inbox_message", 1, Duration.ofSeconds(15));
    awaitCount("scheduler.job_definition", 1, Duration.ofSeconds(15));
    awaitCount("scheduler.trigger_definition", 1, Duration.ofSeconds(15));

    assertThat(count("scheduler.inbox_message")).isEqualTo(1);
    assertThat(count("scheduler.job_definition")).isEqualTo(1);
    assertThat(count("scheduler.trigger_definition")).isEqualTo(1);
    assertThat(count("scheduler.outbox_event")).isEqualTo(1);
  }

  @Test
  void invalidCommandRollsBackInboxAndPublishesStableDeadLetterHeaders() throws Exception {
    Map<String, Object> consumerProperties =
        KafkaTestUtils.consumerProps(
            embeddedKafka, "task-9-dlt-reader-" + UUID.randomUUID(), false);
    DefaultKafkaConsumerFactory<String, String> consumerFactory =
        new DefaultKafkaConsumerFactory<>(
            consumerProperties, new StringDeserializer(), new StringDeserializer());

    try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
      embeddedKafka.consumeFromAnEmbeddedTopic(consumer, DLT_TOPIC);
      kafkaTemplate.send(COMMAND_TOPIC, "billing:" + REQUEST_ID, invalidCommandEnvelope()).get();

      ConsumerRecord<String, String> deadLetter =
          KafkaTestUtils.getSingleRecord(consumer, DLT_TOPIC);

      assertThat(headerText(deadLetter, KafkaTopicConfiguration.MESSAGE_ID_HEADER))
          .isEqualTo(MESSAGE_ID.toString());
      assertThat(headerText(deadLetter, KafkaTopicConfiguration.REQUEST_ID_HEADER))
          .isEqualTo(REQUEST_ID.toString());
      assertThat(headerText(deadLetter, KafkaTopicConfiguration.ERROR_CODE_HEADER))
          .isEqualTo("UNKNOWN_COMMAND_TYPE");
      assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
      assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
      assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
      assertThat(count("scheduler.inbox_message")).isZero();
    }
  }

  private String createScheduleEnvelope() {
    return """
        {
          "schemaVersion": 1,
          "messageId": "%s",
          "requestId": "%s",
          "occurredAt": "2026-08-07T04:00:00Z",
          "producer": "billing-service",
          "namespace": "billing",
          "commandType": "CREATE_SCHEDULE",
          "payload": {
            "job": {
              "jobId": "%s",
              "namespace": "billing",
              "name": "invoice-dispatch",
              "description": "Dispatch invoice events",
              "destinationId": "%s",
              "destinationVersion": 1,
              "eventType": "billing.invoice.due",
              "payload": {"invoiceId": "INV-2030-001"},
              "headers": {"tenant": "mcs"},
              "concurrencyPolicy": "DISALLOW",
              "recoveryPolicy": "REQUEST_RECOVERY",
              "durable": true
            },
            "triggers": [
              {
                "triggerId": "%s",
                "jobId": "%s",
                "namespace": "billing",
                "name": "invoice-once",
                "description": "One-shot invoice trigger",
                "spec": {"type": "ONCE", "fireAt": "2030-01-01T00:05:00Z"},
                "startAt": "2030-01-01T00:00:00Z",
                "endAt": null,
                "priority": 5,
                "timezone": null,
                "misfirePolicy": "FIRE_NOW",
                "calendarNames": []
              }
            ]
          }
        }
        """
        .formatted(MESSAGE_ID, REQUEST_ID, JOB_ID, DESTINATION_ID, TRIGGER_ID, JOB_ID);
  }

  private String invalidCommandEnvelope() {
    return """
        {
          "schemaVersion": 1,
          "messageId": "%s",
          "requestId": "%s",
          "occurredAt": "2026-08-07T04:00:00Z",
          "producer": "billing-service",
          "namespace": "billing",
          "commandType": "UNKNOWN_COMMAND",
          "payload": {}
        }
        """
        .formatted(MESSAGE_ID, REQUEST_ID);
  }

  private void awaitCount(String table, int expected, Duration timeout)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      if (count(table) == expected) {
        return;
      }
      Thread.sleep(100);
    }
    assertThat(count(table)).as(table + " row count").isEqualTo(expected);
  }

  private int count(String table) {
    Integer value = jdbc.queryForObject("select count(*) from " + table, Integer.class);
    return value == null ? 0 : value;
  }

  private static String headerText(ConsumerRecord<String, String> record, String headerName) {
    Header header = record.headers().lastHeader(headerName);
    assertThat(header).as(headerName).isNotNull();
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
