package io.k2iot.mcs.scheduler.command;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class JdbcCommandRequestRepository implements CommandRequestRepository {

  private final JdbcClient jdbc;
  private final JsonMapper jsonMapper;

  public JdbcCommandRequestRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    this.jdbc = jdbc;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Optional<CommandRequest> findByRequestId(UUID requestId) {
    return jdbc.sql(
            """
            select command_request_id, request_id, command_type, namespace, aggregate_id,
                   request_hash, payload, status, response_json, requested_at, processed_at,
                   last_error
            from scheduler.command_request
            where request_id = :requestId
            """)
        .param("requestId", requestId.toString())
        .query(this::mapRequest)
        .optional();
  }

  @Override
  public void insert(CommandRequest request) {
    jdbc.sql(
            """
            insert into scheduler.command_request (
                command_request_id, request_id, command_type, namespace, aggregate_id,
                request_hash, payload, status, response_json, requested_at, processed_at,
                last_error)
            values (
                :commandRequestId, :requestId, :commandType, :namespace, :aggregateId,
                :requestHash, cast(:payload as jsonb), :status, null, :requestedAt, null, null)
            """)
        .param("commandRequestId", request.commandRequestId())
        .param("requestId", request.requestId().toString())
        .param("commandType", request.commandType())
        .param("namespace", request.namespace())
        .param("aggregateId", request.aggregateId())
        .param("requestHash", request.requestHash())
        .param("payload", writeJson(request.payload()))
        .param("status", request.status().name())
        .param("requestedAt", request.requestedAt())
        .update();
  }

  @Override
  public void complete(UUID requestId, JsonNode responseJson, Instant processedAt) {
    int updated =
        jdbc.sql(
                """
                update scheduler.command_request
                set status = 'COMPLETED',
                    response_json = cast(:responseJson as jsonb),
                    processed_at = :processedAt,
                    last_error = null
                where request_id = :requestId and status = 'PROCESSING'
                """)
            .param("responseJson", writeJson(responseJson))
            .param("processedAt", processedAt)
            .param("requestId", requestId.toString())
            .update();
    if (updated != 1) {
      throw new IllegalStateException("Command request was not in PROCESSING state: " + requestId);
    }
  }

  private CommandRequest mapRequest(ResultSet resultSet, int rowNumber) throws SQLException {
    var processedAt = resultSet.getTimestamp("processed_at");
    return new CommandRequest(
        resultSet.getObject("command_request_id", UUID.class),
        UUID.fromString(resultSet.getString("request_id")),
        resultSet.getString("command_type"),
        resultSet.getString("namespace"),
        resultSet.getObject("aggregate_id", UUID.class),
        resultSet.getString("request_hash"),
        readJson(resultSet.getString("payload")),
        CommandRequest.Status.valueOf(resultSet.getString("status")),
        readNullableJson(resultSet.getString("response_json")),
        resultSet.getTimestamp("requested_at").toInstant(),
        processedAt == null ? null : processedAt.toInstant(),
        resultSet.getString("last_error"));
  }

  private JsonNode readJson(String json) {
    try {
      return jsonMapper.readTree(json);
    } catch (JacksonException exception) {
      throw new IllegalStateException("Stored command JSON is invalid", exception);
    }
  }

  private JsonNode readNullableJson(String json) {
    return json == null ? null : readJson(json);
  }

  private String writeJson(Object value) {
    try {
      return jsonMapper.writeValueAsString(value);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Command JSON cannot be serialized", exception);
    }
  }
}
