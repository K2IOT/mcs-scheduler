package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import io.k2iot.mcs.scheduler.trigger.CalendarIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.DailyTimeIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.SimpleIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerSpec;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

public final class RestCommandMapper {

  static final int MAX_PAYLOAD_BYTES = 64 * 1024;
  static final int MAX_HEADER_COUNT = 32;
  static final int MAX_HEADER_BYTES = 4 * 1024;

  private final JsonMapper jsonMapper;

  public RestCommandMapper(JsonMapper jsonMapper) {
    this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
  }

  public UUID requestId(String value) {
    try {
      return UUID.fromString(requireText(value, "Idempotency-Key"));
    } catch (IllegalArgumentException exception) {
      throw new RestContractException(
          "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must be a UUID", exception);
    }
  }

  public long expectedRevision(String ifMatch) {
    String value = requireText(ifMatch, "If-Match").trim();
    if (value.startsWith("W/")) {
      value = value.substring(2).trim();
    }
    if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      value = value.substring(1, value.length() - 1);
    }
    try {
      long revision = Long.parseLong(value);
      if (revision < 1) {
        throw new NumberFormatException("revision must be positive");
      }
      return revision;
    } catch (NumberFormatException exception) {
      throw new RestContractException(
          "INVALID_IF_MATCH", "If-Match must contain a positive numeric revision", exception);
    }
  }

  public SchedulerCommands.CreateJob createJob(
      UUID requestId, RestModels.CreateJobRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.CreateJob(
        requestId, toJobDraft(request.job()), requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.UpdateJob updateJob(
      UUID requestId, long expectedRevision, RestModels.UpdateJobRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.UpdateJob(
        requestId,
        expectedRevision,
        toJobDraft(request.job()),
        requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.CreateTrigger createTrigger(
      UUID requestId, RestModels.CreateTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.CreateTrigger(
        requestId, toTriggerDraft(request.trigger()), requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.ReplaceTrigger replaceTrigger(
      UUID requestId, long expectedRevision, RestModels.ReplaceTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.ReplaceTrigger(
        requestId,
        expectedRevision,
        toTriggerDraft(request.trigger()),
        requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.CreateSchedule createSchedule(
      UUID requestId, RestModels.CreateScheduleRequest request) {
    Objects.requireNonNull(request, "request");
    List<RestModels.TriggerDraftRequest> triggerRequests =
        Objects.requireNonNull(request.triggers(), "triggers");
    List<SchedulerCommands.TriggerDraft> triggers =
        triggerRequests.stream().map(this::toTriggerDraft).toList();
    return new SchedulerCommands.CreateSchedule(
        requestId,
        toJobDraft(request.job()),
        triggers,
        requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.JobMutation jobMutation(
      UUID requestId,
      UUID jobId,
      long expectedRevision,
      RestModels.MutationRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.JobMutation(
        requestId,
        Objects.requireNonNull(jobId, "jobId"),
        requireText(request.namespace(), "namespace"),
        expectedRevision,
        requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.TriggerMutation triggerMutation(
      UUID requestId,
      UUID triggerId,
      long expectedRevision,
      RestModels.MutationRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.TriggerMutation(
        requestId,
        Objects.requireNonNull(triggerId, "triggerId"),
        requireText(request.namespace(), "namespace"),
        expectedRevision,
        requireText(request.caller(), "caller"));
  }

  public SchedulerCommands.FireTriggerNow fireTriggerNow(
      UUID requestId, RestModels.FireTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.FireTriggerNow(
        requestId,
        Objects.requireNonNull(request.triggerId(), "triggerId"),
        requireText(request.namespace(), "namespace"),
        Objects.requireNonNull(request.manualFireId(), "manualFireId"),
        requireText(request.caller(), "caller"));
  }

  public void requireResourceId(UUID pathId, UUID bodyId) {
    if (!Objects.equals(pathId, bodyId)) {
      throw new RestContractException(
          "RESOURCE_ID_MISMATCH", "Resource ID in the request body must match the path");
    }
  }

  private SchedulerCommands.JobDraft toJobDraft(RestModels.JobRequest request) {
    Objects.requireNonNull(request, "job");
    Map<String, Object> payload = Objects.requireNonNull(request.payload(), "payload");
    Map<String, String> headers = Objects.requireNonNull(request.headers(), "headers");
    validatePayload(payload);
    validateHeaders(headers);
    return new SchedulerCommands.JobDraft(
        request.jobId(),
        request.namespace(),
        request.name(),
        request.description(),
        request.destinationId(),
        request.destinationVersion(),
        request.eventType(),
        payload,
        headers,
        request.concurrencyPolicy(),
        request.recoveryPolicy(),
        request.durable());
  }

  private SchedulerCommands.TriggerDraft toTriggerDraft(RestModels.TriggerDraftRequest request) {
    Objects.requireNonNull(request, "trigger");
    return new SchedulerCommands.TriggerDraft(
        request.triggerId(),
        request.jobId(),
        request.namespace(),
        request.name(),
        request.description(),
        toTriggerSpec(request.spec()),
        request.startAt(),
        request.endAt(),
        request.priority(),
        request.timezone(),
        request.misfirePolicy(),
        request.calendarNames() == null ? Set.of() : request.calendarNames());
  }

  private TriggerSpec toTriggerSpec(RestModels.TriggerRequest request) {
    Objects.requireNonNull(request, "trigger spec");
    if (request instanceof RestModels.OnceTriggerRequest once) {
      return new OnceTriggerSpec(once.fireAt());
    }
    if (request instanceof RestModels.CronTriggerRequest cron) {
      return new CronTriggerSpec(cron.expression());
    }
    if (request instanceof RestModels.SimpleIntervalTriggerRequest simple) {
      return new SimpleIntervalTriggerSpec(simple.interval(), simple.repeatCount());
    }
    if (request instanceof RestModels.CalendarIntervalTriggerRequest calendar) {
      return new CalendarIntervalTriggerSpec(calendar.interval(), calendar.unit());
    }
    if (request instanceof RestModels.DailyTimeIntervalTriggerRequest daily) {
      return new DailyTimeIntervalTriggerSpec(
          daily.interval(),
          daily.unit(),
          daily.daysOfWeek(),
          daily.startTime(),
          daily.endTime());
    }
    throw new RestContractException("INVALID_TRIGGER_TYPE", "Unsupported trigger type");
  }

  private void validatePayload(Map<String, Object> payload) {
    try {
      if (jsonMapper.writeValueAsBytes(payload).length > MAX_PAYLOAD_BYTES) {
        throw new RestContractException(
            "PAYLOAD_TOO_LARGE", "Job payload must not exceed 64 KiB");
      }
    } catch (JacksonException exception) {
      throw new RestContractException(
          "INVALID_PAYLOAD", "Job payload cannot be serialized as JSON", exception);
    }
  }

  private static void validateHeaders(Map<String, String> headers) {
    if (headers.size() > MAX_HEADER_COUNT) {
      throw new RestContractException(
          "TOO_MANY_HEADERS", "Job headers must not contain more than 32 entries");
    }
    int bytes =
        headers.entrySet().stream()
            .mapToInt(
                entry ->
                    utf8Length(entry.getKey())
                        + utf8Length(Objects.requireNonNull(entry.getValue(), "header value")))
            .sum();
    if (bytes > MAX_HEADER_BYTES) {
      throw new RestContractException(
          "HEADERS_TOO_LARGE", "Job headers must not exceed 4 KiB in total");
    }
  }

  private static int utf8Length(String value) {
    return Objects.requireNonNull(value, "header key").getBytes(StandardCharsets.UTF_8).length;
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new RestContractException("INVALID_REQUEST", field + " must not be blank");
    }
    return value;
  }

  static final class RestContractException extends IllegalArgumentException {

    private final String code;

    RestContractException(String code, String message) {
      super(message);
      this.code = Objects.requireNonNull(code, "code");
    }

    RestContractException(String code, String message, Throwable cause) {
      super(message, cause);
      this.code = Objects.requireNonNull(code, "code");
    }

    String code() {
      return code;
    }
  }
}
