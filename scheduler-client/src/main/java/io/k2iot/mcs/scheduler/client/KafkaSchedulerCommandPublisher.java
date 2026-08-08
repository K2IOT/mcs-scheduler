package io.k2iot.mcs.scheduler.client;

import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit;
import io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.DailyTimeIntervalUnit;
import io.k2iot.mcs.scheduler.v1.DayOfWeek;
import io.k2iot.mcs.scheduler.v1.JobDraft;
import io.k2iot.mcs.scheduler.v1.MisfirePolicy;
import io.k2iot.mcs.scheduler.v1.RecoveryPolicy;
import io.k2iot.mcs.scheduler.v1.TriggerDraft;
import io.k2iot.mcs.scheduler.v1.TriggerSpec;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.springframework.kafka.core.KafkaTemplate;

public final class KafkaSchedulerCommandPublisher implements AsyncSchedulerClient {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;
  private final String producer;
  private final Supplier<UUID> requestIds;
  private final Supplier<UUID> messageIds;
  private final Clock clock;
  private final JsonFormat.Printer jsonPrinter =
      JsonFormat.printer().omittingInsignificantWhitespace();

  public KafkaSchedulerCommandPublisher(
      KafkaTemplate<String, String> kafkaTemplate, String topic, String producer) {
    this(kafkaTemplate, topic, producer, UUID::randomUUID, UUID::randomUUID, Clock.systemUTC());
  }

  KafkaSchedulerCommandPublisher(
      KafkaTemplate<String, String> kafkaTemplate,
      String topic,
      String producer,
      Supplier<UUID> requestIds,
      Supplier<UUID> messageIds,
      Clock clock) {
    this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
    this.topic = requireText(topic, "topic");
    this.producer = requireText(producer, "producer");
    this.requestIds = Objects.requireNonNull(requestIds, "requestIds");
    this.messageIds = Objects.requireNonNull(messageIds, "messageIds");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public CommandReceipt createSchedule(CreateScheduleRequest request, UUID requestId) {
    Objects.requireNonNull(request, "request");
    String namespace = requireText(request.getNamespace(), "namespace");
    String payload =
        "{\"job\":"
            + jobJson(namespace, request.getJob())
            + ",\"triggers\":["
            + request.getTriggersList().stream()
                .map(trigger -> triggerJson(namespace, trigger))
                .collect(Collectors.joining(","))
            + "]}";
    return publish(
        "CREATE_SCHEDULE",
        namespace,
        request.getCaller().isBlank() ? producer : request.getCaller(),
        payload,
        aggregateKey(namespace, request.getJob().getJobId()),
        requestId);
  }

  @Override
  public CommandReceipt pauseJob(
      UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    return publishJobMutation("PAUSE_JOB", jobId, namespace, expectedRevision, requestId);
  }

  @Override
  public CommandReceipt resumeJob(
      UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    return publishJobMutation("RESUME_JOB", jobId, namespace, expectedRevision, requestId);
  }

  @Override
  public CommandReceipt deleteJob(
      UUID jobId, String namespace, long expectedRevision, boolean cascade, UUID requestId) {
    if (cascade) {
      throw new IllegalArgumentException("cascade job deletion is not supported in scheduler V1");
    }
    return publishJobMutation("DELETE_JOB", jobId, namespace, expectedRevision, requestId);
  }

  @Override
  public CommandReceipt fireTrigger(
      UUID triggerId, String namespace, UUID manualFireId, UUID requestId) {
    Objects.requireNonNull(triggerId, "triggerId");
    Objects.requireNonNull(manualFireId, "manualFireId");
    String normalizedNamespace = requireText(namespace, "namespace");
    String payload =
        "{\"triggerId\":\"" + triggerId + "\",\"manualFireId\":\"" + manualFireId + "\"}";
    return publish(
        "FIRE_TRIGGER_NOW",
        normalizedNamespace,
        producer,
        payload,
        normalizedNamespace + ":" + triggerId,
        requestId);
  }

  private CommandReceipt publishJobMutation(
      String commandType, UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    Objects.requireNonNull(jobId, "jobId");
    String normalizedNamespace = requireText(namespace, "namespace");
    long revision = positiveRevision(expectedRevision);
    String payload = "{\"jobId\":\"" + jobId + "\",\"expectedRevision\":" + revision + "}";
    return publish(
        commandType,
        normalizedNamespace,
        producer,
        payload,
        normalizedNamespace + ":" + jobId,
        requestId);
  }

  private CommandReceipt publish(
      String commandType,
      String namespace,
      String caller,
      String payload,
      String key,
      UUID suppliedRequestId) {
    UUID effectiveRequestId =
        suppliedRequestId != null
            ? suppliedRequestId
            : Objects.requireNonNull(requestIds.get(), "requestId");
    UUID messageId = Objects.requireNonNull(messageIds.get(), "messageId");
    Instant occurredAt = clock.instant();
    String envelope =
        "{\"schemaVersion\":1,\"messageId\":\""
            + messageId
            + "\",\"requestId\":\""
            + effectiveRequestId
            + "\",\"occurredAt\":\""
            + occurredAt
            + "\",\"producer\":\""
            + escape(requireText(caller, "caller"))
            + "\",\"namespace\":\""
            + escape(namespace)
            + "\",\"commandType\":\""
            + commandType
            + "\",\"payload\":"
            + payload
            + "}";
    kafkaTemplate.send(topic, key, envelope);
    return new CommandReceipt(effectiveRequestId, messageId, topic);
  }

  private String jobJson(String namespace, JobDraft job) {
    Objects.requireNonNull(job, "job");
    StringJoiner fields = new StringJoiner(",", "{", "}");
    fields.add(field("jobId", requireText(job.getJobId(), "job.job_id")));
    fields.add(field("namespace", namespace));
    fields.add(field("name", requireText(job.getName(), "job.name")));
    if (!job.getDescription().isEmpty()) {
      fields.add(field("description", job.getDescription()));
    }
    fields.add(field("destinationId", requireText(job.getDestinationId(), "job.destination_id")));
    fields.add("\"destinationVersion\":" + job.getDestinationVersion());
    fields.add(field("eventType", requireText(job.getEventType(), "job.event_type")));
    fields.add("\"payload\":" + structJson(job.getPayload()));
    fields.add("\"headers\":" + mapJson(job.getHeadersMap()));
    fields.add(field("concurrencyPolicy", concurrencyPolicy(job.getConcurrencyPolicy())));
    fields.add(field("recoveryPolicy", recoveryPolicy(job.getRecoveryPolicy())));
    fields.add("\"durable\":" + (job.hasDurable() && job.getDurable()));
    return fields.toString();
  }

  private String triggerJson(String namespace, TriggerDraft trigger) {
    Objects.requireNonNull(trigger, "trigger");
    StringJoiner fields = new StringJoiner(",", "{", "}");
    fields.add(field("triggerId", requireText(trigger.getTriggerId(), "trigger.trigger_id")));
    fields.add(field("jobId", requireText(trigger.getJobId(), "trigger.job_id")));
    fields.add(field("namespace", namespace));
    fields.add(field("name", requireText(trigger.getName(), "trigger.name")));
    if (!trigger.getDescription().isEmpty()) {
      fields.add(field("description", trigger.getDescription()));
    }
    fields.add("\"spec\":" + triggerSpecJson(trigger.getSpec()));
    if (trigger.hasStartAt()) {
      fields.add(field("startAt", instant(trigger.getStartAt()).toString()));
    }
    if (trigger.hasEndAt()) {
      fields.add(field("endAt", instant(trigger.getEndAt()).toString()));
    }
    fields.add("\"priority\":" + (trigger.hasPriority() ? trigger.getPriority() : 5));
    String timezone = triggerTimezone(trigger.getSpec());
    if (timezone != null) {
      fields.add(field("timezone", timezone));
    }
    fields.add(field("misfirePolicy", misfirePolicy(trigger.getMisfirePolicy())));
    fields.add("\"calendarNames\":" + stringArray(trigger.getCalendarNamesList()));
    return fields.toString();
  }

  private String triggerSpecJson(TriggerSpec spec) {
    Objects.requireNonNull(spec, "trigger.spec");
    return switch (spec.getKindCase()) {
      case ONCE -> {
        if (!spec.getOnce().hasFireAt()) {
          throw new IllegalArgumentException("trigger.spec.once.fire_at is required");
        }
        yield "{\"type\":\"ONCE\",\"fireAt\":"
            + quote(instant(spec.getOnce().getFireAt()).toString())
            + "}";
      }
      case CRON ->
          "{\"type\":\"CRON\",\"expression\":"
              + quote(requireText(spec.getCron().getExpression(), "cron.expression"))
              + "}";
      case SIMPLE_INTERVAL -> {
        if (!spec.getSimpleInterval().hasInterval()) {
          throw new IllegalArgumentException("simple_interval.interval is required");
        }
        StringJoiner fields = new StringJoiner(",", "{", "}");
        fields.add(field("type", "SIMPLE_INTERVAL"));
        fields.add(field("interval", duration(spec.getSimpleInterval().getInterval()).toString()));
        if (spec.getSimpleInterval().hasRepeatCount()) {
          fields.add("\"repeatCount\":" + spec.getSimpleInterval().getRepeatCount());
        }
        yield fields.toString();
      }
      case CALENDAR_INTERVAL ->
          "{\"type\":\"CALENDAR_INTERVAL\",\"interval\":"
              + spec.getCalendarInterval().getInterval()
              + ",\"unit\":"
              + quote(calendarUnit(spec.getCalendarInterval().getUnit()))
              + "}";
      case DAILY_TIME_INTERVAL -> {
        var daily = spec.getDailyTimeInterval();
        StringJoiner fields = new StringJoiner(",", "{", "}");
        fields.add(field("type", "DAILY_TIME_INTERVAL"));
        fields.add("\"interval\":" + daily.getInterval());
        fields.add(field("unit", dailyUnit(daily.getUnit())));
        fields.add(
            "\"daysOfWeek\":"
                + daily.getDaysOfWeekList().stream()
                    .map(this::dayOfWeek)
                    .map(this::quote)
                    .collect(Collectors.joining(",", "[", "]")));
        fields.add(field("startTime", localTime(daily.getStartTime()).toString()));
        fields.add(field("endTime", localTime(daily.getEndTime()).toString()));
        yield fields.toString();
      }
      case KIND_NOT_SET -> throw new IllegalArgumentException("trigger.spec kind is required");
    };
  }

  private String triggerTimezone(TriggerSpec spec) {
    return switch (spec.getKindCase()) {
      case CRON -> requireText(spec.getCron().getTimezone(), "cron.timezone");
      case CALENDAR_INTERVAL ->
          requireText(spec.getCalendarInterval().getTimezone(), "calendar_interval.timezone");
      case DAILY_TIME_INTERVAL ->
          requireText(spec.getDailyTimeInterval().getTimezone(), "daily_time_interval.timezone");
      case ONCE, SIMPLE_INTERVAL -> null;
      case KIND_NOT_SET -> throw new IllegalArgumentException("trigger.spec kind is required");
    };
  }

  private String structJson(Struct value) {
    try {
      return jsonPrinter.print(value);
    } catch (Exception exception) {
      throw new IllegalArgumentException(
          "Unable to serialize scheduler command payload", exception);
    }
  }

  private String mapJson(Map<String, String> values) {
    return values.entrySet().stream()
        .map(
            entry ->
                field(entry.getKey(), Objects.requireNonNull(entry.getValue(), "header value")))
        .collect(Collectors.joining(",", "{", "}"));
  }

  private String stringArray(Iterable<String> values) {
    StringJoiner items = new StringJoiner(",", "[", "]");
    values.forEach(value -> items.add(quote(value)));
    return items.toString();
  }

  private String concurrencyPolicy(ConcurrencyPolicy policy) {
    return switch (policy) {
      case CONCURRENCY_POLICY_ALLOW -> "ALLOW";
      case CONCURRENCY_POLICY_DISALLOW -> "DISALLOW";
      case CONCURRENCY_POLICY_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("job.concurrency_policy is required");
    };
  }

  private String recoveryPolicy(RecoveryPolicy policy) {
    return switch (policy) {
      case RECOVERY_POLICY_NONE -> "NONE";
      case RECOVERY_POLICY_REQUEST_RECOVERY -> "REQUEST_RECOVERY";
      case RECOVERY_POLICY_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("job.recovery_policy is required");
    };
  }

  private String misfirePolicy(MisfirePolicy policy) {
    if (policy == MisfirePolicy.MISFIRE_POLICY_UNSPECIFIED) {
      return "SMART_POLICY";
    }
    if (policy == MisfirePolicy.UNRECOGNIZED) {
      throw new IllegalArgumentException("trigger.misfire_policy is invalid");
    }
    return policy.name().substring("MISFIRE_POLICY_".length());
  }

  private String calendarUnit(CalendarIntervalUnit unit) {
    return switch (unit) {
      case CALENDAR_INTERVAL_UNIT_DAY -> "DAYS";
      case CALENDAR_INTERVAL_UNIT_WEEK -> "WEEKS";
      case CALENDAR_INTERVAL_UNIT_MONTH -> "MONTHS";
      case CALENDAR_INTERVAL_UNIT_YEAR -> "YEARS";
      case CALENDAR_INTERVAL_UNIT_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("calendar_interval.unit is required");
    };
  }

  private String dailyUnit(DailyTimeIntervalUnit unit) {
    return switch (unit) {
      case DAILY_TIME_INTERVAL_UNIT_SECOND -> "SECONDS";
      case DAILY_TIME_INTERVAL_UNIT_MINUTE -> "MINUTES";
      case DAILY_TIME_INTERVAL_UNIT_HOUR -> "HOURS";
      case DAILY_TIME_INTERVAL_UNIT_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("daily_time_interval.unit is required");
    };
  }

  private String dayOfWeek(DayOfWeek day) {
    if (day == DayOfWeek.DAY_OF_WEEK_UNSPECIFIED || day == DayOfWeek.UNRECOGNIZED) {
      throw new IllegalArgumentException("daily_time_interval.days_of_week contains invalid value");
    }
    return day.name().substring("DAY_OF_WEEK_".length());
  }

  private Instant instant(Timestamp value) {
    try {
      return Instant.ofEpochSecond(value.getSeconds(), value.getNanos());
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("timestamp is out of range", exception);
    }
  }

  private java.time.Duration duration(com.google.protobuf.Duration value) {
    try {
      return java.time.Duration.ofSeconds(value.getSeconds(), value.getNanos());
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("duration is out of range", exception);
    }
  }

  private LocalTime localTime(io.k2iot.mcs.scheduler.v1.LocalTime value) {
    try {
      return LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), value.getNano());
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("local time is invalid", exception);
    }
  }

  private String aggregateKey(String namespace, String aggregateId) {
    if (aggregateId != null && !aggregateId.isBlank()) {
      return namespace + ":" + aggregateId;
    }
    return namespace + ":schedule";
  }

  private String field(String name, String value) {
    return quote(name) + ":" + quote(value);
  }

  private String quote(String value) {
    return "\"" + escape(Objects.requireNonNull(value, "json string")) + "\"";
  }

  private String escape(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      switch (character) {
        case '\\' -> escaped.append("\\\\");
        case '"' -> escaped.append("\\\"");
        case '\b' -> escaped.append("\\b");
        case '\f' -> escaped.append("\\f");
        case '\n' -> escaped.append("\\n");
        case '\r' -> escaped.append("\\r");
        case '\t' -> escaped.append("\\t");
        default -> {
          if (character < 0x20) {
            escaped.append(String.format("\\u%04x", (int) character));
          } else {
            escaped.append(character);
          }
        }
      }
    }
    return escaped.toString();
  }

  private static long positiveRevision(long revision) {
    if (revision < 1) {
      throw new IllegalArgumentException("expectedRevision must be positive");
    }
    return revision;
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
