package io.k2iot.mcs.scheduler.outbox;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

@FunctionalInterface
public interface OutboxRepository {

  void insert(OutboxEvent event);

  record OutboxEvent(
      UUID eventId,
      String aggregateType,
      UUID aggregateId,
      String eventType,
      JsonNode payload,
      Map<String, String> headers,
      Instant occurredAt) {}
}
