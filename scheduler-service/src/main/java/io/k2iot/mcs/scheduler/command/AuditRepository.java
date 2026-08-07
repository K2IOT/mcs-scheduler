package io.k2iot.mcs.scheduler.command;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

public interface AuditRepository {

  void append(AuditEvent event);

  static AuditRepository noop() {
    return event -> {};
  }

  static AuditRepository jdbc(JdbcClient jdbc, JsonMapper jsonMapper) {
    Objects.requireNonNull(jdbc, "jdbc");
    Objects.requireNonNull(jsonMapper, "jsonMapper");
    return event -> {
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("oldRevision", event.oldRevision());
      payload.put("oldState", event.oldState());
      payload.put("newRevision", event.newRevision());
      payload.put("newState", event.newState());
      payload.put("metadata", event.safeMetadata());
      jdbc.sql(
              """
              insert into scheduler.audit_event (
                  audit_event_id, entity_type, entity_id, action, actor, payload,
                  occurred_at, correlation_id)
              values (
                  :auditEventId, :entityType, :entityId, :action, :actor,
                  cast(:payload as jsonb), :occurredAt, :correlationId)
              """)
          .param("auditEventId", UUID.randomUUID())
          .param("entityType", event.aggregateType())
          .param("entityId", event.aggregateId())
          .param("action", event.action())
          .param("actor", event.actor())
          .param("payload", writeJson(jsonMapper, payload))
          .param("occurredAt", event.occurredAt().atOffset(ZoneOffset.UTC))
          .param("correlationId", event.requestId().toString())
          .update();
    };
  }

  private static String writeJson(JsonMapper jsonMapper, Object value) {
    try {
      return jsonMapper.writeValueAsString(value);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Audit metadata cannot be serialized", exception);
    }
  }

  record AuditEvent(
      UUID requestId,
      String aggregateType,
      UUID aggregateId,
      String action,
      String actor,
      Long oldRevision,
      String oldState,
      Long newRevision,
      String newState,
      Instant occurredAt,
      Map<String, Object> safeMetadata) {

    public AuditEvent {
      Objects.requireNonNull(requestId, "requestId");
      aggregateType = requireText(aggregateType, "aggregateType");
      Objects.requireNonNull(aggregateId, "aggregateId");
      action = requireText(action, "action");
      actor = requireText(actor, "actor");
      Objects.requireNonNull(occurredAt, "occurredAt");
      safeMetadata = Map.copyOf(Objects.requireNonNull(safeMetadata, "safeMetadata"));
    }

    private static String requireText(String value, String field) {
      Objects.requireNonNull(value, field);
      if (value.isBlank()) {
        throw new IllegalArgumentException(field + " must not be blank");
      }
      return value;
    }
  }
}
