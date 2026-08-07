package io.k2iot.mcs.scheduler.execution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

public class JdbcExecutionRepository implements ExecutionRepository {

  private static final TypeReference<Map<String, Object>> OBJECT_MAP = new TypeReference<>() {};

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

  @Override
  public Optional<ExecutionRecord> findById(UUID executionId) {
    return jdbc.sql(baseSelect() + " where e.execution_id = :executionId")
        .param("executionId", executionId)
        .query(this::mapExecution)
        .optional();
  }

  @Override
  public List<ExecutionRecord> findPage(
      String namespace, Instant scheduledFireAfter, UUID idAfter, int limit) {
    String cursor = "";
    if (idAfter != null) {
      cursor =
          scheduledFireAfter == null
              ? " and e.scheduled_fire_time is null and e.execution_id > :idAfter"
              : " and (e.scheduled_fire_time > :scheduledFireAfter"
                  + " or (e.scheduled_fire_time = :scheduledFireAfter and e.execution_id > :idAfter)"
                  + " or e.scheduled_fire_time is null)";
    }
    JdbcClient.StatementSpec statement =
        jdbc.sql(
                baseSelect()
                    + " join scheduler.job_definition j on j.job_id = e.job_id"
                    + " where j.namespace = :namespace"
                    + cursor
                    + " order by e.scheduled_fire_time asc nulls last, e.execution_id limit :limit")
            .param("namespace", namespace)
            .param("limit", limit);
    if (idAfter != null) {
      statement = statement.param("idAfter", idAfter);
      if (scheduledFireAfter != null) {
        statement = statement.param("scheduledFireAfter", postgresTimestamp(scheduledFireAfter));
      }
    }
    return statement.query(this::mapExecution).list();
  }

  private String baseSelect() {
    return """
        select e.execution_id, e.job_id, e.trigger_id, e.manual_fire_id,
               e.scheduled_fire_time, e.actual_fire_time, e.status, e.attempt,
               e.payload, e.created_at, e.updated_at
        from scheduler.execution e
        """;
  }

  private ExecutionRecord mapExecution(ResultSet resultSet, int rowNumber) throws SQLException {
    return new ExecutionRecord(
        resultSet.getObject("execution_id", UUID.class),
        resultSet.getObject("job_id", UUID.class),
        resultSet.getObject("trigger_id", UUID.class),
        resultSet.getObject("manual_fire_id", UUID.class),
        nullableInstant(resultSet, "scheduled_fire_time"),
        nullableInstant(resultSet, "actual_fire_time"),
        Status.valueOf(resultSet.getString("status")),
        resultSet.getInt("attempt"),
        readJson(resultSet.getString("payload")),
        resultSet.getTimestamp("created_at").toInstant(),
        resultSet.getTimestamp("updated_at").toInstant());
  }

  private Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
    var timestamp = resultSet.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
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

  private Map<String, Object> readJson(String json) {
    try {
      return jsonMapper.readValue(json, OBJECT_MAP);
    } catch (JacksonException exception) {
      throw new IllegalStateException("Stored execution JSON is invalid", exception);
    }
  }
}
