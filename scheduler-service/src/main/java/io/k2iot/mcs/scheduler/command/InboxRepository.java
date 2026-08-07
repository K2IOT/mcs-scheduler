package io.k2iot.mcs.scheduler.command;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

public interface InboxRepository {

  Optional<UUID> insertIfAbsent(InboxMessage message);

  void insertCommandResult(CommandResult result);

  void markCompleted(UUID inboxMessageId, Instant processedAt);

  record InboxMessage(
      String messageId,
      String source,
      String sourceTopic,
      int sourcePartition,
      long sourceOffset,
      JsonNode payload,
      Map<String, String> headers,
      Instant receivedAt) {}

  record CommandResult(
      UUID eventId,
      UUID requestId,
      String namespace,
      String commandType,
      JsonNode payload,
      Map<String, String> headers,
      Instant occurredAt) {}
}
