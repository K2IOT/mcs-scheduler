package io.k2iot.mcs.scheduler.execution;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

public class JdbcExecutionRepository implements ExecutionRepository {

  private final JdbcClient jdbc;
  private final JsonMapper jsonMapper;

  public JdbcExecutionRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    this.jdbc = jdbc;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public boolean insertIfAbsent(ExecutionRecord execution) {
    int inserted =
        jdbc.sql(
                """
                insert into scheduler.execution (
                    execution_id, job_id, trigger_id, manual_fire_id, scheduled_fire_time,
                    actual_fire_time, status, attempt, payload, created_at, updated_at)
                values (
                    :executionId, :jobId, :triggerId, :manualFireId, :scheduledFireTime,
                    :actualFireTime, :status, :attempt, cast(:payload as jsonb),
                    :createdAt, :updatedAt)
                on conflict do nothing
                """)
            .param("executionId", execution.executionId())
            .param("jobId", execution.jobId())
            .param("triggerId", execution.triggerId())
            .param("manualFireId", execution.manualFireId())
            .param("scheduledFireTime", postgresTimestamp(execution.scheduledFireTime()))
            .param("actualFireTime", postgresTimestamp(execution.actualFireTime()))
            .param("status", execution.status().name())
            .param("attempt", execution.attempt())
            .param("payload", writeJson(execution.payload()))
            .param("createdAt", postgresTimestamp(execution.createdAt()))
            .param("updatedAt", postgresTimestamp(execution.updatedAt()))
            .update();
    return inserted == 1;
  }

  private OffsetDateTime postgresTimestamp(Instant instant) {
    return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
  }

  private String writeJson(Object value) {
    try {
      return jsonMapper.writeValueAsString(value);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Execution JSON cannot be serialized", exception);
    }
  }
}
