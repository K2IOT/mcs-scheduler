package io.k2iot.mcs.scheduler.kafka;

import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import io.k2iot.mcs.scheduler.rest.RestCommandMapper;
import io.k2iot.mcs.scheduler.rest.RestModels;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Component
public class KafkaCommandMapper {

  private final JsonMapper jsonMapper;
  private final RestCommandMapper restMapper;

  public KafkaCommandMapper(JsonMapper jsonMapper, RestCommandMapper restMapper) {
    this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
    this.restMapper = Objects.requireNonNull(restMapper, "restMapper");
  }

  public MappedCommand map(SchedulerCommandEnvelope envelope) {
    Objects.requireNonNull(envelope, "envelope");
    if (envelope.schemaVersion() != 1) {
      throw new KafkaCommandException("UNSUPPORTED_SCHEMA_VERSION", "schemaVersion must be 1");
    }

    uuid(envelope.messageId(), "messageId");
    UUID requestId = uuid(envelope.requestId(), "requestId");
    Objects.requireNonNull(envelope.occurredAt(), "occurredAt");
    String producer = requireText(envelope.producer(), "producer");
    String namespace = requireText(envelope.namespace(), "namespace");
    String commandType = requireText(envelope.commandType(), "commandType");
    JsonNode payload = Objects.requireNonNull(envelope.payload(), "payload");
    if (!payload.isObject()) {
      throw new KafkaCommandException("INVALID_COMMAND_PAYLOAD", "payload must be a JSON object");
    }

    try {
      return switch (commandType) {
        case "CREATE_JOB" -> mapCreateJob(requestId, producer, namespace, payload);
        case "UPDATE_JOB" -> mapUpdateJob(requestId, producer, namespace, payload);
        case "CREATE_TRIGGER" -> mapCreateTrigger(requestId, producer, namespace, payload);
        case "REPLACE_TRIGGER" -> mapReplaceTrigger(requestId, producer, namespace, payload);
        case "CREATE_SCHEDULE" -> mapCreateSchedule(requestId, producer, namespace, payload);
        case "PAUSE_JOB", "RESUME_JOB", "DELETE_JOB" ->
            mapJobMutation(commandType, requestId, producer, namespace, payload);
        case "PAUSE_TRIGGER", "RESUME_TRIGGER", "DELETE_TRIGGER" ->
            mapTriggerMutation(commandType, requestId, producer, namespace, payload);
        case "FIRE_TRIGGER_NOW" -> mapFireTrigger(requestId, producer, namespace, payload);
        default ->
            throw new KafkaCommandException(
                "UNKNOWN_COMMAND_TYPE", "Unsupported commandType: " + commandType);
      };
    } catch (KafkaCommandException exception) {
      throw exception;
    } catch (JacksonException | NullPointerException | IllegalArgumentException exception) {
      throw new KafkaCommandException(
          "INVALID_COMMAND_PAYLOAD", "Invalid payload for commandType " + commandType, exception);
    }
  }

  private MappedCommand mapCreateJob(
      UUID requestId, String producer, String namespace, JsonNode payload) throws JacksonException {
    CreateJobPayload value = jsonMapper.treeToValue(payload, CreateJobPayload.class);
    SchedulerCommands.CreateJob command =
        restMapper.createJob(requestId, new RestModels.CreateJobRequest(producer, value.job()));
    requireNamespace(namespace, command.job().namespace());
    return new MappedCommand("CREATE_JOB", requestId, namespace, command.job().jobId(), command);
  }

  private MappedCommand mapUpdateJob(
      UUID requestId, String producer, String namespace, JsonNode payload) throws JacksonException {
    UpdateJobPayload value = jsonMapper.treeToValue(payload, UpdateJobPayload.class);
    long revision = positiveRevision(value.expectedRevision());
    SchedulerCommands.UpdateJob command =
        restMapper.updateJob(
            requestId, revision, new RestModels.UpdateJobRequest(producer, value.job()));
    requireNamespace(namespace, command.job().namespace());
    return new MappedCommand("UPDATE_JOB", requestId, namespace, command.job().jobId(), command);
  }

  private MappedCommand mapCreateTrigger(
      UUID requestId, String producer, String namespace, JsonNode payload) throws JacksonException {
    CreateTriggerPayload value = jsonMapper.treeToValue(payload, CreateTriggerPayload.class);
    SchedulerCommands.CreateTrigger command =
        restMapper.createTrigger(
            requestId, new RestModels.CreateTriggerRequest(producer, value.trigger()));
    requireNamespace(namespace, command.trigger().namespace());
    return new MappedCommand(
        "CREATE_TRIGGER", requestId, namespace, command.trigger().triggerId(), command);
  }

  private MappedCommand mapReplaceTrigger(
      UUID requestId, String producer, String namespace, JsonNode payload) throws JacksonException {
    ReplaceTriggerPayload value = jsonMapper.treeToValue(payload, ReplaceTriggerPayload.class);
    long revision = positiveRevision(value.expectedRevision());
    SchedulerCommands.ReplaceTrigger command =
        restMapper.replaceTrigger(
            requestId, revision, new RestModels.ReplaceTriggerRequest(producer, value.trigger()));
    requireNamespace(namespace, command.trigger().namespace());
    return new MappedCommand(
        "REPLACE_TRIGGER", requestId, namespace, command.trigger().triggerId(), command);
  }

  private MappedCommand mapCreateSchedule(
      UUID requestId, String producer, String namespace, JsonNode payload) throws JacksonException {
    CreateSchedulePayload value = jsonMapper.treeToValue(payload, CreateSchedulePayload.class);
    SchedulerCommands.CreateSchedule command =
        restMapper.createSchedule(
            requestId,
            new RestModels.CreateScheduleRequest(producer, value.job(), value.triggers()));
    requireNamespace(namespace, command.job().namespace());
    for (SchedulerCommands.TriggerDraft trigger : command.triggers()) {
      requireNamespace(namespace, trigger.namespace());
    }
    return new MappedCommand(
        "CREATE_SCHEDULE", requestId, namespace, command.job().jobId(), command);
  }

  private MappedCommand mapJobMutation(
      String commandType, UUID requestId, String producer, String namespace, JsonNode payload)
      throws JacksonException {
    JobMutationPayload value = jsonMapper.treeToValue(payload, JobMutationPayload.class);
    SchedulerCommands.JobMutation command =
        restMapper.jobMutation(
            requestId,
            Objects.requireNonNull(value.jobId(), "jobId"),
            positiveRevision(value.expectedRevision()),
            new RestModels.MutationRequest(namespace, producer));
    return new MappedCommand(commandType, requestId, namespace, command.jobId(), command);
  }

  private MappedCommand mapTriggerMutation(
      String commandType, UUID requestId, String producer, String namespace, JsonNode payload)
      throws JacksonException {
    TriggerMutationPayload value = jsonMapper.treeToValue(payload, TriggerMutationPayload.class);
    SchedulerCommands.TriggerMutation command =
        restMapper.triggerMutation(
            requestId,
            Objects.requireNonNull(value.triggerId(), "triggerId"),
            positiveRevision(value.expectedRevision()),
            new RestModels.MutationRequest(namespace, producer));
    return new MappedCommand(commandType, requestId, namespace, command.triggerId(), command);
  }

  private MappedCommand mapFireTrigger(
      UUID requestId, String producer, String namespace, JsonNode payload) throws JacksonException {
    FireTriggerPayload value = jsonMapper.treeToValue(payload, FireTriggerPayload.class);
    SchedulerCommands.FireTriggerNow command =
        restMapper.fireTriggerNow(
            requestId,
            new RestModels.FireTriggerRequest(
                producer,
                namespace,
                Objects.requireNonNull(value.triggerId(), "triggerId"),
                Objects.requireNonNull(value.manualFireId(), "manualFireId")));
    return new MappedCommand(
        "FIRE_TRIGGER_NOW", requestId, namespace, command.triggerId(), command);
  }

  private static void requireNamespace(String envelopeNamespace, String commandNamespace) {
    if (!Objects.equals(envelopeNamespace, commandNamespace)) {
      throw new KafkaCommandException(
          "NAMESPACE_MISMATCH",
          "Envelope namespace must match every namespace inside the command payload");
    }
  }

  private static long positiveRevision(long revision) {
    if (revision < 1) {
      throw new KafkaCommandException(
          "INVALID_COMMAND_PAYLOAD", "expectedRevision must be a positive number");
    }
    return revision;
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

    public String kafkaKey() {
      return namespace + ":" + aggregateId;
    }
  }

  private record CreateJobPayload(RestModels.JobRequest job) {}

  private record UpdateJobPayload(long expectedRevision, RestModels.JobRequest job) {}

  private record CreateTriggerPayload(RestModels.TriggerDraftRequest trigger) {}

  private record ReplaceTriggerPayload(
      long expectedRevision, RestModels.TriggerDraftRequest trigger) {}

  private record CreateSchedulePayload(
      RestModels.JobRequest job, List<RestModels.TriggerDraftRequest> triggers) {}

  private record JobMutationPayload(UUID jobId, long expectedRevision) {}

  private record TriggerMutationPayload(UUID triggerId, long expectedRevision) {}

  private record FireTriggerPayload(UUID triggerId, UUID manualFireId) {}

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
