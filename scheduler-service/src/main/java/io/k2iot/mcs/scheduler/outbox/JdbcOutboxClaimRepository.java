package io.k2iot.mcs.scheduler.outbox;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class JdbcOutboxClaimRepository implements OutboxClaimRepository {

  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  private final JdbcClient jdbc;
  private final JsonMapper jsonMapper;

  public JdbcOutboxClaimRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    this.jdbc = jdbc;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public List<ClaimedOutboxEvent> claimBatch(
      int batchSize, Instant now, Duration claimTimeout, UUID claimId) {
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    Instant claimUntil = now.plus(claimTimeout);
    return jdbc.sql(
            """
            with candidates as (
                select outbox_event_id
                from scheduler.outbox_event
                where (
                    state = 'PENDING'
                    and (next_attempt_at is null or next_attempt_at <= :now)
                ) or (
                    state = 'IN_PROGRESS'
                    and claim_until <= :now
                )
                order by occurred_at, outbox_event_id
                for update skip locked
                limit :batchSize
            )
            update scheduler.outbox_event outbox
            set state = 'IN_PROGRESS',
                publish_attempts = publish_attempts + 1,
                next_attempt_at = null,
                claim_id = :claimId,
                claim_until = :claimUntil,
                last_error = null
            from candidates
            where outbox.outbox_event_id = candidates.outbox_event_id
            returning outbox.outbox_event_id, outbox.aggregate_type, outbox.aggregate_id,
                      outbox.event_type, outbox.payload, outbox.headers, outbox.occurred_at,
                      outbox.publish_attempts, outbox.claim_id
            """)
        .param("now", postgresTimestamp(now))
        .param("batchSize", batchSize)
        .param("claimId", claimId)
        .param("claimUntil", postgresTimestamp(claimUntil))
        .query(this::mapClaimedEvent)
        .list();
  }

  @Override
  public void markPublished(UUID eventId, UUID claimId, Instant publishedAt) {
    int updated =
        jdbc.sql(
                """
                with published as (
                    update scheduler.outbox_event
                    set state = 'PUBLISHED',
                        published_at = :publishedAt,
                        next_attempt_at = null,
                        claim_id = null,
                        claim_until = null,
                        last_error = null
                    where outbox_event_id = :eventId
                      and state = 'IN_PROGRESS'
                      and claim_id = :claimId
                    returning aggregate_type, aggregate_id
                ), execution_update as (
                    update scheduler.execution execution
                    set status = 'DELIVERED', updated_at = :publishedAt
                    from published
                    where published.aggregate_type = 'EXECUTION'
                      and execution.execution_id = published.aggregate_id
                    returning execution.execution_id
                )
                select count(*) from published
                """)
            .param("publishedAt", postgresTimestamp(publishedAt))
            .param("eventId", eventId)
            .param("claimId", claimId)
            .query(Integer.class)
            .single();
    requireClaimUpdate(updated, eventId);
  }

  @Override
  public void reschedule(UUID eventId, UUID claimId, Instant nextAttemptAt, String error) {
    int updated =
        jdbc.sql(
                """
                update scheduler.outbox_event
                set state = 'PENDING',
                    next_attempt_at = :nextAttemptAt,
                    claim_id = null,
                    claim_until = null,
                    last_error = :error
                where outbox_event_id = :eventId
                  and state = 'IN_PROGRESS'
                  and claim_id = :claimId
                """)
            .param("nextAttemptAt", postgresTimestamp(nextAttemptAt))
            .param("error", error)
            .param("eventId", eventId)
            .param("claimId", claimId)
            .update();
    requireClaimUpdate(updated, eventId);
  }

  @Override
  public void markDead(UUID eventId, UUID claimId, Instant failedAt, String error) {
    int updated =
        jdbc.sql(
                """
                with dead as (
                    update scheduler.outbox_event
                    set state = 'DEAD',
                        published_at = null,
                        next_attempt_at = null,
                        claim_id = null,
                        claim_until = null,
                        last_error = :error
                    where outbox_event_id = :eventId
                      and state = 'IN_PROGRESS'
                      and claim_id = :claimId
                    returning aggregate_type, aggregate_id
                ), execution_update as (
                    update scheduler.execution execution
                    set status = 'DELIVERY_FAILED', updated_at = :failedAt
                    from dead
                    where dead.aggregate_type = 'EXECUTION'
                      and execution.execution_id = dead.aggregate_id
                    returning execution.execution_id
                )
                select count(*) from dead
                """)
            .param("failedAt", postgresTimestamp(failedAt))
            .param("error", error)
            .param("eventId", eventId)
            .param("claimId", claimId)
            .query(Integer.class)
            .single();
    requireClaimUpdate(updated, eventId);
  }

  private ClaimedOutboxEvent mapClaimedEvent(ResultSet resultSet, int rowNumber)
      throws SQLException {
    return new ClaimedOutboxEvent(
        resultSet.getObject("outbox_event_id", UUID.class),
        resultSet.getString("aggregate_type"),
        resultSet.getObject("aggregate_id", UUID.class),
        resultSet.getString("event_type"),
        readJson(resultSet.getString("payload")),
        readStringMap(resultSet.getString("headers")),
        resultSet.getTimestamp("occurred_at").toInstant(),
        resultSet.getInt("publish_attempts"),
        resultSet.getObject("claim_id", UUID.class));
  }

  private JsonNode readJson(String json) {
    try {
      return jsonMapper.readTree(json);
    } catch (JacksonException exception) {
      throw new IllegalStateException("Stored outbox payload is invalid JSON", exception);
    }
  }

  private Map<String, String> readStringMap(String json) {
    try {
      return jsonMapper.readValue(json, STRING_MAP);
    } catch (JacksonException exception) {
      throw new IllegalStateException("Stored outbox headers are invalid JSON", exception);
    }
  }

  private static OffsetDateTime postgresTimestamp(Instant instant) {
    return instant.atOffset(ZoneOffset.UTC);
  }

  private static void requireClaimUpdate(int updated, UUID eventId) {
    if (updated != 1) {
      throw new IllegalStateException("Outbox claim is no longer owned: " + eventId);
    }
  }
}
