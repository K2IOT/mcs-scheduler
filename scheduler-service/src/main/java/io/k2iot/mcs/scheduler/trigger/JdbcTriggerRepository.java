package io.k2iot.mcs.scheduler.trigger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

public class JdbcTriggerRepository implements TriggerRepository {

  private static final TypeReference<Set<String>> STRING_SET = new TypeReference<>() {};

  private final JdbcClient jdbc;
  private final JsonMapper jsonMapper;

  public JdbcTriggerRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    this.jdbc = jdbc;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Optional<TriggerDefinition> findById(UUID triggerId) {
    return jdbc.sql(baseSelect() + " where trigger_id = :triggerId")
        .param("triggerId", triggerId)
        .query(this::mapTrigger)
        .optional();
  }

  @Override
  public List<TriggerDefinition> findByJobId(UUID jobId) {
    return jdbc.sql(baseSelect() + " where job_id = :jobId order by created_at, trigger_id")
        .param("jobId", jobId)
        .query(this::mapTrigger)
        .list();
  }

  @Override
  public void insert(TriggerDefinition definition) {
    jdbc.sql(
            """
            insert into scheduler.trigger_definition (
                trigger_id, job_id, namespace, name, description, type, spec,
                start_at, end_at, priority, timezone, misfire_policy, calendar_names,
                state, revision, created_at, created_by, updated_at, updated_by,
                deleted_at, deleted_by)
            values (
                :triggerId, :jobId, :namespace, :name, :description, :type,
                cast(:spec as jsonb), :startAt, :endAt, :priority, :timezone,
                :misfirePolicy, cast(:calendarNames as jsonb), :state, :revision,
                :createdAt, :createdBy, :updatedAt, :updatedBy, :deletedAt, :deletedBy)
            """)
        .param("triggerId", definition.triggerId())
        .param("jobId", definition.jobId())
        .param("namespace", definition.namespace())
        .param("name", definition.name())
        .param("description", definition.description())
        .param("type", definition.type().name())
        .param("spec", writeJson(definition.spec()))
        .param("startAt", postgresTimestamp(definition.startAt()))
        .param("endAt", postgresTimestamp(definition.endAt()))
        .param("priority", definition.priority())
        .param("timezone", definition.timezone())
        .param("misfirePolicy", definition.misfirePolicy().name())
        .param("calendarNames", writeJson(definition.calendarNames()))
        .param("state", definition.state().name())
        .param("revision", definition.revision())
        .param("createdAt", postgresTimestamp(definition.createdAt()))
        .param("createdBy", definition.createdBy())
        .param("updatedAt", postgresTimestamp(definition.updatedAt()))
        .param("updatedBy", definition.updatedBy())
        .param("deletedAt", postgresTimestamp(deletedAt(definition)))
        .param("deletedBy", deletedBy(definition))
        .update();
  }

  @Override
  public boolean update(TriggerDefinition definition, long expectedRevision) {
    int updated =
        jdbc.sql(
                """
                update scheduler.trigger_definition
                set job_id = :jobId,
                    namespace = :namespace,
                    name = :name,
                    description = :description,
                    type = :type,
                    spec = cast(:spec as jsonb),
                    start_at = :startAt,
                    end_at = :endAt,
                    priority = :priority,
                    timezone = :timezone,
                    misfire_policy = :misfirePolicy,
                    calendar_names = cast(:calendarNames as jsonb),
                    state = :state,
                    revision = :revision,
                    updated_at = :updatedAt,
                    updated_by = :updatedBy,
                    deleted_at = :deletedAt,
                    deleted_by = :deletedBy
                where trigger_id = :triggerId and revision = :expectedRevision
                """)
            .param("jobId", definition.jobId())
            .param("namespace", definition.namespace())
            .param("name", definition.name())
            .param("description", definition.description())
            .param("type", definition.type().name())
            .param("spec", writeJson(definition.spec()))
            .param("startAt", postgresTimestamp(definition.startAt()))
            .param("endAt", postgresTimestamp(definition.endAt()))
            .param("priority", definition.priority())
            .param("timezone", definition.timezone())
            .param("misfirePolicy", definition.misfirePolicy().name())
            .param("calendarNames", writeJson(definition.calendarNames()))
            .param("state", definition.state().name())
            .param("revision", definition.revision())
            .param("updatedAt", postgresTimestamp(definition.updatedAt()))
            .param("updatedBy", definition.updatedBy())
            .param("deletedAt", postgresTimestamp(deletedAt(definition)))
            .param("deletedBy", deletedBy(definition))
            .param("triggerId", definition.triggerId())
            .param("expectedRevision", expectedRevision)
            .update();
    return updated == 1;
  }

  private String baseSelect() {
    return """
        select trigger_id, job_id, namespace, name, description, type, spec,
               start_at, end_at, priority, timezone, misfire_policy, calendar_names,
               state, revision, created_at, created_by, updated_at, updated_by
        from scheduler.trigger_definition
        """;
  }

  private TriggerDefinition mapTrigger(ResultSet resultSet, int rowNumber) throws SQLException {
    TriggerDefinition.Type type = TriggerDefinition.Type.valueOf(resultSet.getString("type"));
    return new TriggerDefinition(
        resultSet.getObject("trigger_id", UUID.class),
        resultSet.getObject("job_id", UUID.class),
        resultSet.getString("namespace"),
        resultSet.getString("name"),
        resultSet.getString("description"),
        readSpec(type, resultSet.getString("spec")),
        nullableInstant(resultSet, "start_at"),
        nullableInstant(resultSet, "end_at"),
        resultSet.getInt("priority"),
        resultSet.getString("timezone"),
        TriggerDefinition.MisfirePolicy.valueOf(resultSet.getString("misfire_policy")),
        readJson(resultSet.getString("calendar_names"), STRING_SET),
        TriggerDefinition.State.valueOf(resultSet.getString("state")),
        resultSet.getLong("revision"),
        resultSet.getTimestamp("created_at").toInstant(),
        resultSet.getString("created_by"),
        resultSet.getTimestamp("updated_at").toInstant(),
        resultSet.getString("updated_by"));
  }

  private TriggerSpec readSpec(TriggerDefinition.Type type, String json) {
    Class<? extends TriggerSpec> specClass =
        switch (type) {
          case ONCE -> OnceTriggerSpec.class;
          case CRON -> CronTriggerSpec.class;
          case SIMPLE_INTERVAL -> SimpleIntervalTriggerSpec.class;
          case CALENDAR_INTERVAL -> CalendarIntervalTriggerSpec.class;
          case DAILY_TIME_INTERVAL -> DailyTimeIntervalTriggerSpec.class;
        };
    try {
      return jsonMapper.readValue(json, specClass);
    } catch (JacksonException exception) {
      throw new IllegalStateException("Stored trigger specification is invalid", exception);
    }
  }

  private Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
    var timestamp = resultSet.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
  }

  private OffsetDateTime postgresTimestamp(Instant instant) {
    return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
  }

  private Instant deletedAt(TriggerDefinition definition) {
    return definition.state() == TriggerDefinition.State.DELETED ? definition.updatedAt() : null;
  }

  private String deletedBy(TriggerDefinition definition) {
    return definition.state() == TriggerDefinition.State.DELETED ? definition.updatedBy() : null;
  }

  private String writeJson(Object value) {
    try {
      return jsonMapper.writeValueAsString(value);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Trigger JSON cannot be serialized", exception);
    }
  }

  private <T> T readJson(String json, TypeReference<T> type) {
    try {
      return jsonMapper.readValue(json, type);
    } catch (JacksonException exception) {
      throw new IllegalStateException("Stored trigger JSON is invalid", exception);
    }
  }
}
