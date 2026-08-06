package io.k2iot.mcs.scheduler.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public final class JdbcJobRepository implements JobRepository {

  private static final TypeReference<Map<String, Object>> OBJECT_MAP = new TypeReference<>() {};
  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  private final JdbcClient jdbc;
  private final ObjectMapper objectMapper;

  public JdbcJobRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.objectMapper = objectMapper;
  }

  @Override
  public Optional<JobDefinition> findById(UUID jobId) {
    return jdbc.sql(
            """
            select job_id, namespace, name, description, destination_id, destination_version,
                   event_type, payload, headers, concurrency_policy, recovery_policy, durable,
                   state, revision, created_at, created_by, updated_at, updated_by
            from scheduler.job_definition
            where job_id = :jobId
            """)
        .param("jobId", jobId)
        .query(this::mapJob)
        .optional();
  }

  @Override
  public void insert(JobDefinition definition) {
    jdbc.sql(
            """
            insert into scheduler.job_definition (
                job_id, namespace, name, description, destination_id, destination_version,
                event_type, payload, headers, concurrency_policy, recovery_policy, durable,
                state, revision, created_at, created_by, updated_at, updated_by,
                deleted_at, deleted_by)
            values (
                :jobId, :namespace, :name, :description, :destinationId, :destinationVersion,
                :eventType, cast(:payload as jsonb), cast(:headers as jsonb),
                :concurrencyPolicy, :recoveryPolicy, :durable, :state, :revision,
                :createdAt, :createdBy, :updatedAt, :updatedBy, :deletedAt, :deletedBy)
            """)
        .param("jobId", definition.jobId())
        .param("namespace", definition.namespace())
        .param("name", definition.name())
        .param("description", definition.description())
        .param("destinationId", definition.destinationId())
        .param("destinationVersion", definition.destinationVersion())
        .param("eventType", definition.eventType())
        .param("payload", writeJson(definition.payload()))
        .param("headers", writeJson(definition.headers()))
        .param("concurrencyPolicy", definition.concurrencyPolicy().name())
        .param("recoveryPolicy", definition.recoveryPolicy().name())
        .param("durable", definition.durable())
        .param("state", definition.state().name())
        .param("revision", definition.revision())
        .param("createdAt", definition.createdAt())
        .param("createdBy", definition.createdBy())
        .param("updatedAt", definition.updatedAt())
        .param("updatedBy", definition.updatedBy())
        .param("deletedAt", deletedAt(definition))
        .param("deletedBy", deletedBy(definition))
        .update();
  }

  @Override
  public boolean update(JobDefinition definition, long expectedRevision) {
    int updated =
        jdbc.sql(
                """
                update scheduler.job_definition
                set namespace = :namespace,
                    name = :name,
                    description = :description,
                    destination_id = :destinationId,
                    destination_version = :destinationVersion,
                    event_type = :eventType,
                    payload = cast(:payload as jsonb),
                    headers = cast(:headers as jsonb),
                    concurrency_policy = :concurrencyPolicy,
                    recovery_policy = :recoveryPolicy,
                    durable = :durable,
                    state = :state,
                    revision = :revision,
                    updated_at = :updatedAt,
                    updated_by = :updatedBy,
                    deleted_at = :deletedAt,
                    deleted_by = :deletedBy
                where job_id = :jobId and revision = :expectedRevision
                """)
            .param("namespace", definition.namespace())
            .param("name", definition.name())
            .param("description", definition.description())
            .param("destinationId", definition.destinationId())
            .param("destinationVersion", definition.destinationVersion())
            .param("eventType", definition.eventType())
            .param("payload", writeJson(definition.payload()))
            .param("headers", writeJson(definition.headers()))
            .param("concurrencyPolicy", definition.concurrencyPolicy().name())
            .param("recoveryPolicy", definition.recoveryPolicy().name())
            .param("durable", definition.durable())
            .param("state", definition.state().name())
            .param("revision", definition.revision())
            .param("updatedAt", definition.updatedAt())
            .param("updatedBy", definition.updatedBy())
            .param("deletedAt", deletedAt(definition))
            .param("deletedBy", deletedBy(definition))
            .param("jobId", definition.jobId())
            .param("expectedRevision", expectedRevision)
            .update();
    return updated == 1;
  }

  private JobDefinition mapJob(ResultSet resultSet, int rowNumber) throws SQLException {
    return new JobDefinition(
        resultSet.getObject("job_id", UUID.class),
        resultSet.getString("namespace"),
        resultSet.getString("name"),
        resultSet.getString("description"),
        resultSet.getObject("destination_id", UUID.class),
        resultSet.getLong("destination_version"),
        resultSet.getString("event_type"),
        readJson(resultSet.getString("payload"), OBJECT_MAP),
        readJson(resultSet.getString("headers"), STRING_MAP),
        ConcurrencyPolicy.valueOf(resultSet.getString("concurrency_policy")),
        RecoveryPolicy.valueOf(resultSet.getString("recovery_policy")),
        resultSet.getBoolean("durable"),
        JobDefinition.State.valueOf(resultSet.getString("state")),
        resultSet.getLong("revision"),
        resultSet.getTimestamp("created_at").toInstant(),
        resultSet.getString("created_by"),
        resultSet.getTimestamp("updated_at").toInstant(),
        resultSet.getString("updated_by"));
  }

  private Instant deletedAt(JobDefinition definition) {
    return definition.state() == JobDefinition.State.DELETED ? definition.updatedAt() : null;
  }

  private String deletedBy(JobDefinition definition) {
    return definition.state() == JobDefinition.State.DELETED ? definition.updatedBy() : null;
  }

  private String writeJson(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Job JSON cannot be serialized", exception);
    }
  }

  private <T> T readJson(String json, TypeReference<T> type) {
    try {
      return objectMapper.readValue(json, type);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Stored job JSON is invalid", exception);
    }
  }
}
