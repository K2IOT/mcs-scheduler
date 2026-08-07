package io.k2iot.mcs.scheduler.outbox;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

public class JdbcOutboxRepository implements OutboxRepository {

  private final JdbcClient jdbc;
  private final JsonMapper jsonMapper;

  public JdbcOutboxRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    this.jdbc = jdbc;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void insert(OutboxEvent event) {
    jdbc.sql(
            """
            insert into scheduler.outbox_event (
                outbox_event_id, aggregate_type, aggregate_id, event_type,
                payload, headers, occurred_at, published_at, publish_attempts, last_error)
            values (
                :eventId, :aggregateType, :aggregateId, :eventType,
                cast(:payload as jsonb), cast(:headers as jsonb), :occurredAt,
                null, 0, null)
            """)
        .param("eventId", event.eventId())
        .param("aggregateType", event.aggregateType())
        .param("aggregateId", event.aggregateId())
        .param("eventType", event.eventType())
        .param("payload", writeJson(event.payload()))
        .param("headers", writeJson(event.headers()))
        .param("occurredAt", postgresTimestamp(event.occurredAt()))
        .update();
  }

  private OffsetDateTime postgresTimestamp(Instant instant) {
    return instant.atOffset(ZoneOffset.UTC);
  }

  private String writeJson(Object value) {
    try {
      return jsonMapper.writeValueAsString(value);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Outbox JSON cannot be serialized", exception);
    }
  }
}
