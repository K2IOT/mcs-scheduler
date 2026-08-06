package io.k2iot.mcs.scheduler.destination;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record DestinationDefinition(
    UUID destinationId,
    long version,
    String namespace,
    Type type,
    String topic,
    String keyExpression,
    Map<String, String> headers,
    boolean enabled,
    Instant createdAt,
    String createdBy,
    Instant updatedAt,
    String updatedBy) {

  public DestinationDefinition {
    Objects.requireNonNull(destinationId, "destinationId");
    if (version < 1) {
      throw new IllegalArgumentException("version must be positive");
    }
    namespace = requireText(namespace, "namespace");
    Objects.requireNonNull(type, "type");
    topic = requireText(topic, "topic");
    headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
    Objects.requireNonNull(createdAt, "createdAt");
    createdBy = requireText(createdBy, "createdBy");
    Objects.requireNonNull(updatedAt, "updatedAt");
    updatedBy = requireText(updatedBy, "updatedBy");
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }

  public enum Type {
    KAFKA
  }
}
