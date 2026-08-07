package io.k2iot.mcs.scheduler.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.rest.RestCommandMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class KafkaCommandMapperTest {

  private static final UUID MESSAGE_ID = UUID.fromString("61000000-0000-4000-8000-000000000001");
  private static final UUID REQUEST_ID = UUID.fromString("62000000-0000-4000-8000-000000000001");
  private static final UUID JOB_ID = UUID.fromString("63000000-0000-4000-8000-000000000001");
  private static final UUID DESTINATION_ID =
      UUID.fromString("64000000-0000-4000-8000-000000000001");
  private static final Instant OCCURRED_AT = Instant.parse("2026-08-07T04:00:00Z");

  private final JsonMapper jsonMapper = JsonMapper.builder().build();
  private final KafkaCommandMapper mapper =
      new KafkaCommandMapper(jsonMapper, new RestCommandMapper(jsonMapper));

  @Test
  void rejectsUnsupportedSchemaVersion() {
    KafkaCommandMapper.KafkaCommandException failure =
        failure(envelope(2, MESSAGE_ID.toString(), "UNKNOWN_COMMAND", jsonMapper.createObjectNode()));

    assertThat(failure.code()).isEqualTo("UNSUPPORTED_SCHEMA_VERSION");
  }

  @Test
  void rejectsMalformedMessageId() {
    KafkaCommandMapper.KafkaCommandException failure =
        failure(envelope(1, "not-a-uuid", "UNKNOWN_COMMAND", jsonMapper.createObjectNode()));

    assertThat(failure.code()).isEqualTo("INVALID_UUID");
  }

  @Test
  void rejectsUnknownCommandType() {
    KafkaCommandMapper.KafkaCommandException failure =
        failure(
            envelope(
                1, MESSAGE_ID.toString(), "UNKNOWN_COMMAND", jsonMapper.createObjectNode()));

    assertThat(failure.code()).isEqualTo("UNKNOWN_COMMAND_TYPE");
  }

  @Test
  void rejectsNestedNamespaceMismatch() throws Exception {
    JsonNode payload =
        jsonMapper.readTree(
            """
            {
              "job": {
                "jobId": "%s",
                "namespace": "other-namespace",
                "name": "invoice-dispatch",
                "description": "Dispatch invoice events",
                "destinationId": "%s",
                "destinationVersion": 1,
                "eventType": "billing.invoice.due",
                "payload": {},
                "headers": {},
                "concurrencyPolicy": "DISALLOW",
                "recoveryPolicy": "REQUEST_RECOVERY",
                "durable": true
              }
            }
            """
                .formatted(JOB_ID, DESTINATION_ID));

    KafkaCommandMapper.KafkaCommandException failure =
        failure(envelope(1, MESSAGE_ID.toString(), "CREATE_JOB", payload));

    assertThat(failure.code()).isEqualTo("NAMESPACE_MISMATCH");
  }

  private SchedulerCommandEnvelope envelope(
      int schemaVersion, String messageId, String commandType, JsonNode payload) {
    return new SchedulerCommandEnvelope(
        schemaVersion,
        messageId,
        REQUEST_ID.toString(),
        OCCURRED_AT,
        "billing-service",
        "billing",
        commandType,
        payload);
  }

  private KafkaCommandMapper.KafkaCommandException failure(SchedulerCommandEnvelope envelope) {
    try {
      mapper.map(envelope);
      throw new AssertionError("Expected KafkaCommandException");
    } catch (KafkaCommandMapper.KafkaCommandException exception) {
      return exception;
    }
  }
}
