package io.k2iot.mcs.scheduler.execution;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.outbox.OutboxRepository;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ExecutionEventFactory {

  private static final String OUTBOX_EVENT_TYPE = "SCHEDULED_EXECUTION";
  private static final String AGGREGATE_TYPE = "EXECUTION";

  private final JsonMapper jsonMapper;

  public ExecutionEventFactory(JsonMapper jsonMapper) {
    this.jsonMapper = jsonMapper;
  }

  public OutboxRepository.OutboxEvent create(
      UUID executionId,
      UUID jobId,
      UUID triggerId,
      String namespace,
      String eventType,
      Instant scheduledFireTime,
      Instant actualFireTime,
      boolean recovery,
      JobDefinition job,
      DestinationDefinition destination) {
    Map<String, String> eventHeaders = new LinkedHashMap<>(destination.headers());
    eventHeaders.putAll(job.headers());

    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("schemaVersion", 1);
    envelope.put("executionId", executionId.toString());
    envelope.put("namespace", namespace);
    envelope.put("eventType", eventType);
    envelope.put("jobId", jobId.toString());
    if (triggerId != null) {
      envelope.put("triggerId", triggerId.toString());
    }
    if (scheduledFireTime != null) {
      envelope.put("scheduledFireTime", scheduledFireTime.toString());
    }
    envelope.put("actualFireTime", actualFireTime.toString());
    envelope.put("recovery", recovery);
    envelope.put("payload", job.payload());
    envelope.put("headers", Map.copyOf(eventHeaders));

    Map<String, String> routingHeaders = new LinkedHashMap<>();
    routingHeaders.put("topic", destination.topic());
    routingHeaders.put("key", executionId.toString());
    if (destination.keyExpression() != null && !destination.keyExpression().isBlank()) {
      routingHeaders.put("keyExpression", destination.keyExpression());
    }

    JsonNode payload = jsonMapper.valueToTree(envelope);
    return new OutboxRepository.OutboxEvent(
        executionId,
        AGGREGATE_TYPE,
        executionId,
        OUTBOX_EVENT_TYPE,
        payload,
        Map.copyOf(routingHeaders),
        actualFireTime);
  }
}
