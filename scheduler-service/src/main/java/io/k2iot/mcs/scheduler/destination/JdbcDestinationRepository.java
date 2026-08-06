package io.k2iot.mcs.scheduler.destination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public final class JdbcDestinationRepository implements DestinationRepository {

  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  private final JdbcClient jdbc;
  private final ObjectMapper objectMapper;

  public JdbcDestinationRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
    this.jdbc = jdbc;
    this.objectMapper = objectMapper;
  }

  @Override
  public Optional<DestinationDefinition> findByIdAndVersion(UUID destinationId, long version) {
    return jdbc.sql(
            """
            select destination_id, version, namespace, type, topic, key_expression, headers,
                   enabled, created_at, created_by, updated_at, updated_by
            from scheduler.destination
            where destination_id = :destinationId and version = :version
            """)
        .param("destinationId", destinationId)
        .param("version", version)
        .query(this::mapDestination)
        .optional();
  }

  private DestinationDefinition mapDestination(ResultSet resultSet, int rowNumber)
      throws SQLException {
    return new DestinationDefinition(
        resultSet.getObject("destination_id", UUID.class),
        resultSet.getLong("version"),
        resultSet.getString("namespace"),
        DestinationDefinition.Type.valueOf(resultSet.getString("type")),
        resultSet.getString("topic"),
        resultSet.getString("key_expression"),
        readStringMap(resultSet.getString("headers")),
        resultSet.getBoolean("enabled"),
        resultSet.getTimestamp("created_at").toInstant(),
        resultSet.getString("created_by"),
        resultSet.getTimestamp("updated_at").toInstant(),
        resultSet.getString("updated_by"));
  }

  private Map<String, String> readStringMap(String json) {
    try {
      return objectMapper.readValue(json, STRING_MAP);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Stored destination headers are invalid JSON", exception);
    }
  }
}
