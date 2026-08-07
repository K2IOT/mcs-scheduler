package io.k2iot.mcs.scheduler.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

public interface OutboxClaimRepository {

  List<ClaimedOutboxEvent> claimBatch(
      int batchSize, Instant now, Duration claimTimeout, UUID claimId);

  void markPublished(UUID eventId, UUID claimId, Instant publishedAt);

  void reschedule(UUID eventId, UUID claimId, Instant nextAttemptAt, String error);

  void markDead(UUID eventId, UUID claimId, Instant failedAt, String error);

  record ClaimedOutboxEvent(
      UUID eventId,
      String aggregateType,
      UUID aggregateId,
      String eventType,
      JsonNode payload,
      Map<String, String> headers,
      Instant occurredAt,
      int publishAttempts,
      UUID claimId) {}
}
