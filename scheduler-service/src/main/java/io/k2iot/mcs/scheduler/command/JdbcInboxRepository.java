package io.k2iot.mcs.scheduler.command;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

public class JdbcInboxRepository implements InboxRepository {

  private final JdbcClient jdbc;
  private final JsonMapper jsonMapper;

  public JdbcInboxRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    this.jdbc = jdbc;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Optional<UUID> insertIfAbsent(InboxMessage message) {
    UUID inboxMessageId = UUID.randomUUID();
    int inserted =
        jdbc.sql(
                """
                insert into scheduler.inbox_message (
                    inbox_message_id, message_id, source, source_topic, source_partition,
                    source_offset, payload, headers, received_at, processed_at, last_error)
                values (
                    :inboxMessageId, :messageId, :source, :sourceTopic, :sourcePartition,
                    :sourceOffset, cast(:payload as jsonb), cast(:headers as jsonb),
                    :receivedAt, null, null)
                on conflict do nothing
                """)
            .param("inboxMessageId", inboxMessageId)
            .param("messageId", message.messageId())
            .param("source", message.source())
            .param("sourceTopic", message.sourceTopic())
            .param("sourcePartition", message.sourcePartition())
            .param("sourceOffset", message.sourceOffset())
            .param("payload", writeJson(message.payload()))
            .param("headers", writeJson(message.headers()))
            .param("receivedAt", postgresTimestamp(message.receivedAt()))
            .update();
    return inserted == 1 ? Optional.of(inboxMessageId) : Optional.empty();
  }

  @Override
  public void insertCommandResult(CommandResult result) {
    jdbc.sql(
            """
            insert into scheduler.outbox_event (
                outbox_event_id, aggregate_type, aggregate_id, event_type,
                payload, headers, occurred_at, published_at, publish_attempts, last_error)
            values (
                :eventId, 'COMMAND_REQUEST', :requestId, 'SCHEDULER_COMMAND_RESULT',
                cast(:payload as jsonb), cast(:headers as jsonb), :occurredAt,
                null, 0, null)
            """)
        .param("eventId", result.eventId())
        .param("requestId", result.requestId())
        .param("payload", writeJson(result.payload()))
        .param("headers", writeJson(result.headers()))
        .param("occurredAt", postgresTimestamp(result.occurredAt()))
        .update();
  }

  @Override
  public void markCompleted(UUID inboxMessageId, Instant processedAt) {
    int updated =
        jdbc.sql(
                """
                update scheduler.inbox_message
                set processed_at = :processedAt, last_error = null
                where inbox_message_id = :inboxMessageId and processed_at is null
                """)
            .param("processedAt", postgresTimestamp(processedAt))
            .param("inboxMessageId", inboxMessageId)
            .update();
    if (updated != 1) {
      throw new IllegalStateException("Inbox message was not pending: " + inboxMessageId);
    }
  }

  private OffsetDateTime postgresTimestamp(Instant instant) {
    return instant.atOffset(ZoneOffset.UTC);
  }

  private String writeJson(Object value) {
    try {
      return jsonMapper.writeValueAsString(value);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Inbox/outbox JSON cannot be serialized", exception);
    }
  }
}
