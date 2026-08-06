package io.k2iot.mcs.scheduler.command;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

public record CommandRequest(
    UUID commandRequestId,
    UUID requestId,
    String commandType,
    String namespace,
    UUID aggregateId,
    String requestHash,
    JsonNode payload,
    Status status,
    JsonNode responseJson,
    Instant requestedAt,
    Instant processedAt,
    String lastError) {

  private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

  public CommandRequest {
    Objects.requireNonNull(commandRequestId, "commandRequestId");
    Objects.requireNonNull(requestId, "requestId");
    commandType = requireText(commandType, "commandType");
    namespace = requireText(namespace, "namespace");
    requestHash = requireText(requestHash, "requestHash");
    if (!SHA_256.matcher(requestHash).matches()) {
      throw new IllegalArgumentException("requestHash must be a lowercase SHA-256 value");
    }
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(requestedAt, "requestedAt");
  }

  public static CommandRequest processing(
      UUID commandRequestId,
      UUID requestId,
      String commandType,
      String namespace,
      UUID aggregateId,
      String requestHash,
      JsonNode payload,
      Instant requestedAt) {
    return new CommandRequest(
        commandRequestId,
        requestId,
        commandType,
        namespace,
        aggregateId,
        requestHash,
        payload,
        Status.PROCESSING,
        null,
        requestedAt,
        null,
        null);
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }

  public enum Status {
    RECEIVED,
    PROCESSING,
    COMPLETED,
    FAILED
  }
}
