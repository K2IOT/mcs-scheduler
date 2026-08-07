package io.k2iot.mcs.scheduler.kafka;

import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class KafkaCommandMapper {

  private final JsonMapper jsonMapper;

  public KafkaCommandMapper(JsonMapper jsonMapper) {
    this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
  }

  public MappedCommand map(SchedulerCommandEnvelope envelope) {
    Objects.requireNonNull(envelope, "envelope");
    if (envelope.schemaVersion() != 1) {
      throw new KafkaCommandException("UNSUPPORTED_SCHEMA_VERSION", "schemaVersion must be 1");
    }
    UUID requestId = uuid(envelope.requestId(), "requestId");
    requireText(envelope.messageId(), "messageId");
    requireText(envelope.producer(), "producer");
    String namespace = requireText(envelope.namespace(), "namespace");
    String commandType = requireText(envelope.commandType(), "commandType");
    if (!"CREATE_SCHEDULE".equals(commandType)) {
      throw new KafkaCommandException("UNKNOWN_COMMAND_TYPE", "Unsupported commandType: " + commandType);
    }
    throw new KafkaCommandException(
        "INVALID_COMMAND_PAYLOAD", "CREATE_SCHEDULE payload mapping is not implemented yet");
  }

  JsonMapper jsonMapper() {
    return jsonMapper;
  }

  private static UUID uuid(String value, String field) {
    try {
      return UUID.fromString(requireText(value, field));
    } catch (IllegalArgumentException exception) {
      throw new KafkaCommandException("INVALID_UUID", field + " must be a UUID", exception);
    }
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new KafkaCommandException("INVALID_COMMAND", field + " must not be blank");
    }
    return value;
  }

  public record MappedCommand(
      String commandType, UUID requestId, String namespace, UUID aggregateId, Object command) {
    public MappedCommand {
      commandType = requireText(commandType, "commandType");
      Objects.requireNonNull(requestId, "requestId");
      namespace = requireText(namespace, "namespace");
      Objects.requireNonNull(aggregateId, "aggregateId");
      Objects.requireNonNull(command, "command");
    }
  }

  public static final class KafkaCommandException extends IllegalArgumentException {

    private final String code;

    public KafkaCommandException(String code, String message) {
      super(message);
      this.code = Objects.requireNonNull(code, "code");
    }

    public KafkaCommandException(String code, String message, Throwable cause) {
      super(message, cause);
      this.code = Objects.requireNonNull(code, "code");
    }

    public String code() {
      return code;
    }
  }
}
