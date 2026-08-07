package io.k2iot.mcs.scheduler.grpc;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.k2iot.mcs.scheduler.command.ManualFireResult;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.CalendarIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.DailyTimeIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.SimpleIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerSpec;
import io.k2iot.mcs.scheduler.v1.CreateJobRequest;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.CreateTriggerRequest;
import io.k2iot.mcs.scheduler.v1.DeleteJobRequest;
import io.k2iot.mcs.scheduler.v1.DeleteTriggerRequest;
import io.k2iot.mcs.scheduler.v1.ExecutionResponse;
import io.k2iot.mcs.scheduler.v1.FireTriggerRequest;
import io.k2iot.mcs.scheduler.v1.JobMutationRequest;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ReplaceTriggerRequest;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.TriggerMutationRequest;
import io.k2iot.mcs.scheduler.v1.TriggerResponse;
import io.k2iot.mcs.scheduler.v1.UpdateJobRequest;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
public final class GrpcCommandMapper {

  private static final int MAX_PAYLOAD_BYTES = 64 * 1024;
  private static final int MAX_HEADER_COUNT = 32;
  private static final int MAX_HEADER_BYTES = 4 * 1024;
  private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();

  private static final Context.Key<String> REQUEST_ID_CONTEXT = Context.key("mcs-request-id");
  private static final Context.Key<String> CALLER_CONTEXT = Context.key("mcs-caller");
  private static final Metadata.Key<String> REQUEST_ID_HEADER =
      Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CALLER_HEADER =
      Metadata.Key.of("x-mcs-caller", Metadata.ASCII_STRING_MARSHALLER);

  @Bean
  @GlobalServerInterceptor
  ServerInterceptor grpcRequestMetadataInterceptor() {
    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Context context = Context.current();
        String requestId = headers.get(REQUEST_ID_HEADER);
        String caller = headers.get(CALLER_HEADER);
        if (requestId != null && !requestId.isBlank()) {
          context = context.withValue(REQUEST_ID_CONTEXT, requestId);
        }
        if (caller != null && !caller.isBlank()) {
          context = context.withValue(CALLER_CONTEXT, caller);
        }
        return Contexts.interceptCall(context, call, headers, next);
      }
    };
  }

  SchedulerCommands.CreateJob createJob(CreateJobRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.CreateJob(
        requestId(request.getRequestId()),
        jobDraft(request.getNamespace(), request.getJob()),
        actor(request.getCaller()));
  }

  SchedulerCommands.UpdateJob updateJob(UpdateJobRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.UpdateJob(
        requestId(request.getRequestId()),
        expectedRevision(request.getExpectedRevision()),
        jobDraft(request.getNamespace(), request.getJob()),
        actor(request.getCaller()));
  }

  SchedulerCommands.JobMutation jobMutation(JobMutationRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.JobMutation(
        requestId(request.getRequestId()),
        uuid(request.getJobId(), "job_id"),
        requireText(request.getNamespace(), "namespace"),
        expectedRevision(request.getExpectedRevision()),
        actor(request.getCaller()));
  }

  SchedulerCommands.JobMutation deleteJob(DeleteJobRequest request) {
    Objects.requireNonNull(request, "request");
    if (request.getCascade()) {
      throw new IllegalArgumentException("cascade job deletion is not supported in scheduler V1");
    }
    return new SchedulerCommands.JobMutation(
        requestId(request.getRequestId()),
        uuid(request.getJobId(), "job_id"),
        requireText(request.getNamespace(), "namespace"),
        expectedRevision(request.getExpectedRevision()),
        actor(request.getCaller()));
  }

  SchedulerCommands.CreateTrigger createTrigger(CreateTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.CreateTrigger(
        requestId(request.getRequestId()),
        triggerDraft(request.getNamespace(), request.getTrigger()),
        actor(request.getCaller()));
  }

  SchedulerCommands.ReplaceTrigger replaceTrigger(ReplaceTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.ReplaceTrigger(
        requestId(request.getRequestId()),
        expectedRevision(request.getExpectedRevision()),
        triggerDraft(request.getNamespace(), request.getTrigger()),
        actor(request.getCaller()));
  }

  SchedulerCommands.TriggerMutation triggerMutation(TriggerMutationRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.TriggerMutation(
        requestId(request.getRequestId()),
        uuid(request.getTriggerId(), "trigger_id"),
        requireText(request.getNamespace(), "namespace"),
        expectedRevision(request.getExpectedRevision()),
        actor(request.getCaller()));
  }

  SchedulerCommands.TriggerMutation deleteTrigger(DeleteTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.TriggerMutation(
        requestId(request.getRequestId()),
        uuid(request.getTriggerId(), "trigger_id"),
        requireText(request.getNamespace(), "namespace"),
        expectedRevision(request.getExpectedRevision()),
        actor(request.getCaller()));
  }

  SchedulerCommands.FireTriggerNow fireTriggerNow(FireTriggerRequest request) {
    Objects.requireNonNull(request, "request");
    return new SchedulerCommands.FireTriggerNow(
        requestId(request.getRequestId()),
        uuid(request.getTriggerId(), "trigger_id"),
        requireText(request.getNamespace(), "namespace"),
        uuid(request.getManualFireId(), "manual_fire_id"),
        actor(request.getCaller()));
  }

  SchedulerCommands.CreateSchedule createSchedule(CreateScheduleRequest request) {
    Objects.requireNonNull(request, "request");
    String namespace = requireText(request.getNamespace(), "namespace");
    List<SchedulerCommands.TriggerDraft> triggers =
        request.getTriggersList().stream()
            .map(trigger -> triggerDraft(namespace, trigger))
            .toList();
    return new SchedulerCommands.CreateSchedule(
        requestId(request.getRequestId()),
        jobDraft(namespace, request.getJob()),
        triggers,
        actor(request.getCaller()));
  }

  JobResponse jobResponse(JobDefinition job) {
    return JobResponse.newBuilder().setJob(jobDefinition(job)).build();
  }

  TriggerResponse triggerResponse(TriggerDefinition trigger) {
    return TriggerResponse.newBuilder().setTrigger(triggerDefinition(trigger)).build();
  }

  ScheduleResponse scheduleResponse(SchedulerCommands.ScheduleResult result) {
    return ScheduleResponse.newBuilder()
        .setJob(jobDefinition(result.job()))
        .addAllTriggers(result.triggers().stream().map(this::triggerDefinition).toList())
        .build();
  }

  ExecutionResponse executionResponse(ManualFireResult result) {
    return ExecutionResponse.newBuilder()
        .setManualFireId(result.manualFireId().toString())
        .setTriggerId(result.triggerId().toString())
        .setJobId(result.jobId().toString())
        .build();
  }

  io.k2iot.mcs.scheduler.v1.JobDefinition jobDefinition(JobDefinition job) {
    var builder =
        io.k2iot.mcs.scheduler.v1.JobDefinition.newBuilder()
            .setJobId(job.jobId().toString())
            .setNamespace(job.namespace())
            .setName(job.name())
            .setDestinationId(job.destinationId().toString())
            .setDestinationVersion(job.destinationVersion())
            .setEventType(job.eventType())
            .setPayload(toStruct(job.payload()))
            .putAllHeaders(job.headers())
            .setConcurrencyPolicy(toProtoConcurrency(job.concurrencyPolicy()))
            .setRecoveryPolicy(toProtoRecovery(job.recoveryPolicy()))
            .setDurable(job.durable())
            .setState(toProtoJobState(job.state()))
            .setRevision(job.revision())
            .setCreatedAt(timestamp(job.createdAt()))
            .setCreatedBy(job.createdBy())
            .setUpdatedAt(timestamp(job.updatedAt()))
            .setUpdatedBy(job.updatedBy());
    if (job.description() != null) {
      builder.setDescription(job.description());
    }
    return builder.build();
  }

  io.k2iot.mcs.scheduler.v1.TriggerDefinition triggerDefinition(TriggerDefinition trigger) {
    var builder =
        io.k2iot.mcs.scheduler.v1.TriggerDefinition.newBuilder()
            .setTriggerId(trigger.triggerId().toString())
            .setJobId(trigger.jobId().toString())
            .setNamespace(trigger.namespace())
            .setName(trigger.name())
            .setSpec(toProtoSpec(trigger))
            .setPriority(trigger.priority())
            .setMisfirePolicy(toProtoMisfire(trigger.misfirePolicy()))
            .addAllCalendarNames(trigger.calendarNames())
            .setState(toProtoTriggerState(trigger.state()))
            .setRevision(trigger.revision())
            .setCreatedAt(timestamp(trigger.createdAt()))
            .setCreatedBy(trigger.createdBy())
            .setUpdatedAt(timestamp(trigger.updatedAt()))
            .setUpdatedBy(trigger.updatedBy());
    if (trigger.description() != null) {
      builder.setDescription(trigger.description());
    }
    if (trigger.startAt() != null) {
      builder.setStartAt(timestamp(trigger.startAt()));
    }
    if (trigger.endAt() != null) {
      builder.setEndAt(timestamp(trigger.endAt()));
    }
    return builder.build();
  }

  UUID uuid(String value, String field) {
    try {
      return UUID.fromString(requireText(value, field));
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException(field + " must be a UUID", exception);
    }
  }

  private UUID requestId(String requestValue) {
    return uuid(firstNonBlank(requestValue, REQUEST_ID_CONTEXT.get()), "request_id");
  }

  private String actor(String requestValue) {
    return requireText(firstNonBlank(CALLER_CONTEXT.get(), requestValue), "caller");
  }

  private SchedulerCommands.JobDraft jobDraft(
      String namespace, io.k2iot.mcs.scheduler.v1.JobDraft draft) {
    Objects.requireNonNull(draft, "job");
    Map<String, Object> payload = fromStruct(draft.getPayload());
    Map<String, String> headers = draft.getHeadersMap();
    validatePayload(payload);
    validateHeaders(headers);
    return new SchedulerCommands.JobDraft(
        uuid(draft.getJobId(), "job.job_id"),
        requireText(namespace, "namespace"),
        requireText(draft.getName(), "job.name"),
        emptyToNull(draft.getDescription()),
        uuid(draft.getDestinationId(), "job.destination_id"),
        draft.getDestinationVersion(),
        requireText(draft.getEventType(), "job.event_type"),
        payload,
        headers,
        toDomainConcurrency(draft.getConcurrencyPolicy()),
        toDomainRecovery(draft.getRecoveryPolicy()),
        draft.hasDurable() && draft.getDurable());
  }

  private SchedulerCommands.TriggerDraft triggerDraft(
      String namespace, io.k2iot.mcs.scheduler.v1.TriggerDraft draft) {
    Objects.requireNonNull(draft, "trigger");
    TriggerSpec spec = toDomainSpec(draft.getSpec());
    return new SchedulerCommands.TriggerDraft(
        uuid(draft.getTriggerId(), "trigger.trigger_id"),
        uuid(draft.getJobId(), "trigger.job_id"),
        requireText(namespace, "namespace"),
        requireText(draft.getName(), "trigger.name"),
        emptyToNull(draft.getDescription()),
        spec,
        draft.hasStartAt() ? instant(draft.getStartAt()) : null,
        draft.hasEndAt() ? instant(draft.getEndAt()) : null,
        draft.hasPriority() ? draft.getPriority() : 5,
        timezone(draft.getSpec()),
        toDomainMisfire(draft.getMisfirePolicy()),
        new LinkedHashSet<>(draft.getCalendarNamesList()));
  }

  private TriggerSpec toDomainSpec(io.k2iot.mcs.scheduler.v1.TriggerSpec spec) {
    Objects.requireNonNull(spec, "trigger.spec");
    return switch (spec.getKindCase()) {
      case ONCE -> {
        if (!spec.getOnce().hasFireAt()) {
          throw new IllegalArgumentException("trigger.spec.once.fire_at is required");
        }
        yield new OnceTriggerSpec(instant(spec.getOnce().getFireAt()));
      }
      case CRON ->
          new CronTriggerSpec(requireText(spec.getCron().getExpression(), "cron.expression"));
      case SIMPLE_INTERVAL ->
          new SimpleIntervalTriggerSpec(
              duration(spec.getSimpleInterval().getInterval()),
              spec.getSimpleInterval().hasRepeatCount()
                  ? spec.getSimpleInterval().getRepeatCount()
                  : null);
      case CALENDAR_INTERVAL ->
          new CalendarIntervalTriggerSpec(
              spec.getCalendarInterval().getInterval(),
              toDomainCalendarUnit(spec.getCalendarInterval().getUnit()));
      case DAILY_TIME_INTERVAL ->
          new DailyTimeIntervalTriggerSpec(
              spec.getDailyTimeInterval().getInterval(),
              toDomainDailyUnit(spec.getDailyTimeInterval().getUnit()),
              spec.getDailyTimeInterval().getDaysOfWeekList().stream()
                  .map(this::toDomainDay)
                  .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new)),
              localTime(spec.getDailyTimeInterval().getStartTime()),
              localTime(spec.getDailyTimeInterval().getEndTime()));
      case KIND_NOT_SET -> throw new IllegalArgumentException("trigger.spec kind is required");
    };
  }

  private String timezone(io.k2iot.mcs.scheduler.v1.TriggerSpec spec) {
    return switch (spec.getKindCase()) {
      case CRON -> requireText(spec.getCron().getTimezone(), "cron.timezone");
      case CALENDAR_INTERVAL ->
          requireText(spec.getCalendarInterval().getTimezone(), "calendar_interval.timezone");
      case DAILY_TIME_INTERVAL ->
          requireText(spec.getDailyTimeInterval().getTimezone(), "daily_time_interval.timezone");
      case ONCE, SIMPLE_INTERVAL, KIND_NOT_SET -> null;
    };
  }

  private io.k2iot.mcs.scheduler.v1.TriggerSpec toProtoSpec(TriggerDefinition trigger) {
    var builder = io.k2iot.mcs.scheduler.v1.TriggerSpec.newBuilder();
    if (trigger.spec() instanceof OnceTriggerSpec once) {
      builder.setOnce(
          io.k2iot.mcs.scheduler.v1.OnceTrigger.newBuilder().setFireAt(timestamp(once.fireAt())));
    } else if (trigger.spec() instanceof CronTriggerSpec cron) {
      builder.setCron(
          io.k2iot.mcs.scheduler.v1.CronTrigger.newBuilder()
              .setExpression(cron.expression())
              .setTimezone(requireText(trigger.timezone(), "trigger.timezone")));
    } else if (trigger.spec() instanceof SimpleIntervalTriggerSpec simple) {
      var simpleBuilder =
          io.k2iot.mcs.scheduler.v1.SimpleIntervalTrigger.newBuilder()
              .setInterval(duration(simple.interval()));
      if (simple.repeatCount() != null) {
        simpleBuilder.setRepeatCount(simple.repeatCount());
      }
      builder.setSimpleInterval(simpleBuilder);
    } else if (trigger.spec() instanceof CalendarIntervalTriggerSpec calendar) {
      builder.setCalendarInterval(
          io.k2iot.mcs.scheduler.v1.CalendarIntervalTrigger.newBuilder()
              .setInterval(calendar.interval())
              .setUnit(toProtoCalendarUnit(calendar.unit()))
              .setTimezone(requireText(trigger.timezone(), "trigger.timezone")));
    } else if (trigger.spec() instanceof DailyTimeIntervalTriggerSpec daily) {
      builder.setDailyTimeInterval(
          io.k2iot.mcs.scheduler.v1.DailyTimeIntervalTrigger.newBuilder()
              .setInterval(daily.interval())
              .setUnit(toProtoDailyUnit(daily.unit()))
              .addAllDaysOfWeek(daily.daysOfWeek().stream().map(this::toProtoDay).toList())
              .setStartTime(localTime(daily.startTime()))
              .setEndTime(localTime(daily.endTime()))
              .setTimezone(requireText(trigger.timezone(), "trigger.timezone")));
    } else {
      throw new IllegalArgumentException("Unsupported trigger spec: " + trigger.spec().getClass());
    }
    return builder.build();
  }

  private Map<String, Object> fromStruct(Struct struct) {
    Map<String, Object> result = new LinkedHashMap<>();
    struct.getFieldsMap().forEach((key, value) -> result.put(key, fromValue(value)));
    return result;
  }

  private Object fromValue(Value value) {
    return switch (value.getKindCase()) {
      case NULL_VALUE -> null;
      case NUMBER_VALUE -> value.getNumberValue();
      case STRING_VALUE -> value.getStringValue();
      case BOOL_VALUE -> value.getBoolValue();
      case STRUCT_VALUE -> fromStruct(value.getStructValue());
      case LIST_VALUE -> {
        List<Object> list = new ArrayList<>();
        value.getListValue().getValuesList().forEach(item -> list.add(fromValue(item)));
        yield list;
      }
      case KIND_NOT_SET -> throw new IllegalArgumentException("payload contains an unset value");
    };
  }

  private Struct toStruct(Map<String, Object> values) {
    Struct.Builder builder = Struct.newBuilder();
    values.forEach((key, value) -> builder.putFields(key, toValue(value)));
    return builder.build();
  }

  private Value toValue(Object value) {
    Value.Builder builder = Value.newBuilder();
    if (value == null) {
      return builder.setNullValue(NullValue.NULL_VALUE).build();
    }
    if (value instanceof String string) {
      return builder.setStringValue(string).build();
    }
    if (value instanceof Boolean bool) {
      return builder.setBoolValue(bool).build();
    }
    if (value instanceof Number number) {
      return builder.setNumberValue(number.doubleValue()).build();
    }
    if (value instanceof Map<?, ?> map) {
      Struct.Builder struct = Struct.newBuilder();
      map.forEach((key, nested) -> struct.putFields(Objects.toString(key), toValue(nested)));
      return builder.setStructValue(struct).build();
    }
    if (value instanceof Iterable<?> iterable) {
      ListValue.Builder list = ListValue.newBuilder();
      iterable.forEach(item -> list.addValues(toValue(item)));
      return builder.setListValue(list).build();
    }
    throw new IllegalArgumentException("Unsupported payload value type: " + value.getClass());
  }

  private static Instant instant(Timestamp timestamp) {
    try {
      return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("timestamp is out of range", exception);
    }
  }

  private static Timestamp timestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private static java.time.Duration duration(com.google.protobuf.Duration duration) {
    try {
      return java.time.Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("duration is out of range", exception);
    }
  }

  private static com.google.protobuf.Duration duration(java.time.Duration duration) {
    return com.google.protobuf.Duration.newBuilder()
        .setSeconds(duration.getSeconds())
        .setNanos(duration.getNano())
        .build();
  }

  private static LocalTime localTime(io.k2iot.mcs.scheduler.v1.LocalTime value) {
    try {
      return LocalTime.of(value.getHour(), value.getMinute(), value.getSecond(), value.getNano());
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("local time is invalid", exception);
    }
  }

  private static io.k2iot.mcs.scheduler.v1.LocalTime localTime(LocalTime value) {
    return io.k2iot.mcs.scheduler.v1.LocalTime.newBuilder()
        .setHour(value.getHour())
        .setMinute(value.getMinute())
        .setSecond(value.getSecond())
        .setNano(value.getNano())
        .build();
  }

  private static long expectedRevision(long revision) {
    if (revision < 1) {
      throw new IllegalArgumentException("expected_revision must be positive");
    }
    return revision;
  }

  private static ConcurrencyPolicy toDomainConcurrency(
      io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy policy) {
    return switch (policy) {
      case CONCURRENCY_POLICY_ALLOW -> ConcurrencyPolicy.ALLOW;
      case CONCURRENCY_POLICY_DISALLOW -> ConcurrencyPolicy.DISALLOW;
      case CONCURRENCY_POLICY_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("job.concurrency_policy is required");
    };
  }

  private static io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy toProtoConcurrency(
      ConcurrencyPolicy policy) {
    return switch (policy) {
      case ALLOW -> io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy.CONCURRENCY_POLICY_ALLOW;
      case DISALLOW -> io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy.CONCURRENCY_POLICY_DISALLOW;
    };
  }

  private static RecoveryPolicy toDomainRecovery(io.k2iot.mcs.scheduler.v1.RecoveryPolicy policy) {
    return switch (policy) {
      case RECOVERY_POLICY_NONE -> RecoveryPolicy.NONE;
      case RECOVERY_POLICY_REQUEST_RECOVERY -> RecoveryPolicy.REQUEST_RECOVERY;
      case RECOVERY_POLICY_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("job.recovery_policy is required");
    };
  }

  private static io.k2iot.mcs.scheduler.v1.RecoveryPolicy toProtoRecovery(RecoveryPolicy policy) {
    return switch (policy) {
      case NONE -> io.k2iot.mcs.scheduler.v1.RecoveryPolicy.RECOVERY_POLICY_NONE;
      case REQUEST_RECOVERY ->
          io.k2iot.mcs.scheduler.v1.RecoveryPolicy.RECOVERY_POLICY_REQUEST_RECOVERY;
    };
  }

  private static TriggerDefinition.MisfirePolicy toDomainMisfire(
      io.k2iot.mcs.scheduler.v1.MisfirePolicy policy) {
    if (policy == io.k2iot.mcs.scheduler.v1.MisfirePolicy.MISFIRE_POLICY_UNSPECIFIED) {
      return TriggerDefinition.MisfirePolicy.SMART_POLICY;
    }
    if (policy == io.k2iot.mcs.scheduler.v1.MisfirePolicy.UNRECOGNIZED) {
      throw new IllegalArgumentException("trigger.misfire_policy is invalid");
    }
    return TriggerDefinition.MisfirePolicy.valueOf(
        policy.name().substring("MISFIRE_POLICY_".length()));
  }

  private static io.k2iot.mcs.scheduler.v1.MisfirePolicy toProtoMisfire(
      TriggerDefinition.MisfirePolicy policy) {
    return io.k2iot.mcs.scheduler.v1.MisfirePolicy.valueOf("MISFIRE_POLICY_" + policy.name());
  }

  private static io.k2iot.mcs.scheduler.v1.JobState toProtoJobState(JobDefinition.State state) {
    return io.k2iot.mcs.scheduler.v1.JobState.valueOf("JOB_STATE_" + state.name());
  }

  private static io.k2iot.mcs.scheduler.v1.TriggerState toProtoTriggerState(
      TriggerDefinition.State state) {
    return io.k2iot.mcs.scheduler.v1.TriggerState.valueOf("TRIGGER_STATE_" + state.name());
  }

  private static ChronoUnit toDomainCalendarUnit(
      io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit unit) {
    return switch (unit) {
      case CALENDAR_INTERVAL_UNIT_DAY -> ChronoUnit.DAYS;
      case CALENDAR_INTERVAL_UNIT_WEEK -> ChronoUnit.WEEKS;
      case CALENDAR_INTERVAL_UNIT_MONTH -> ChronoUnit.MONTHS;
      case CALENDAR_INTERVAL_UNIT_YEAR -> ChronoUnit.YEARS;
      case CALENDAR_INTERVAL_UNIT_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("calendar_interval.unit is required");
    };
  }

  private static io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit toProtoCalendarUnit(
      ChronoUnit unit) {
    return switch (unit) {
      case DAYS -> io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit.CALENDAR_INTERVAL_UNIT_DAY;
      case WEEKS -> io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit.CALENDAR_INTERVAL_UNIT_WEEK;
      case MONTHS -> io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit.CALENDAR_INTERVAL_UNIT_MONTH;
      case YEARS -> io.k2iot.mcs.scheduler.v1.CalendarIntervalUnit.CALENDAR_INTERVAL_UNIT_YEAR;
      default -> throw new IllegalArgumentException("Unsupported calendar interval unit: " + unit);
    };
  }

  private static ChronoUnit toDomainDailyUnit(
      io.k2iot.mcs.scheduler.v1.DailyTimeIntervalUnit unit) {
    return switch (unit) {
      case DAILY_TIME_INTERVAL_UNIT_SECOND -> ChronoUnit.SECONDS;
      case DAILY_TIME_INTERVAL_UNIT_MINUTE -> ChronoUnit.MINUTES;
      case DAILY_TIME_INTERVAL_UNIT_HOUR -> ChronoUnit.HOURS;
      case DAILY_TIME_INTERVAL_UNIT_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("daily_time_interval.unit is required");
    };
  }

  private static io.k2iot.mcs.scheduler.v1.DailyTimeIntervalUnit toProtoDailyUnit(ChronoUnit unit) {
    return switch (unit) {
      case SECONDS ->
          io.k2iot.mcs.scheduler.v1.DailyTimeIntervalUnit.DAILY_TIME_INTERVAL_UNIT_SECOND;
      case MINUTES ->
          io.k2iot.mcs.scheduler.v1.DailyTimeIntervalUnit.DAILY_TIME_INTERVAL_UNIT_MINUTE;
      case HOURS -> io.k2iot.mcs.scheduler.v1.DailyTimeIntervalUnit.DAILY_TIME_INTERVAL_UNIT_HOUR;
      default -> throw new IllegalArgumentException("Unsupported daily interval unit: " + unit);
    };
  }

  private DayOfWeek toDomainDay(io.k2iot.mcs.scheduler.v1.DayOfWeek day) {
    if (day == io.k2iot.mcs.scheduler.v1.DayOfWeek.DAY_OF_WEEK_UNSPECIFIED
        || day == io.k2iot.mcs.scheduler.v1.DayOfWeek.UNRECOGNIZED) {
      throw new IllegalArgumentException("daily_time_interval.days_of_week contains invalid value");
    }
    return DayOfWeek.valueOf(day.name().substring("DAY_OF_WEEK_".length()));
  }

  private io.k2iot.mcs.scheduler.v1.DayOfWeek toProtoDay(DayOfWeek day) {
    return io.k2iot.mcs.scheduler.v1.DayOfWeek.valueOf("DAY_OF_WEEK_" + day.name());
  }

  private static void validatePayload(Map<String, Object> payload) {
    try {
      if (JSON_MAPPER.writeValueAsBytes(payload).length > MAX_PAYLOAD_BYTES) {
        throw new IllegalArgumentException("Job payload must not exceed 64 KiB");
      }
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Job payload cannot be serialized as JSON", exception);
    }
  }

  private static void validateHeaders(Map<String, String> headers) {
    if (headers.size() > MAX_HEADER_COUNT) {
      throw new IllegalArgumentException("Job headers must not contain more than 32 entries");
    }
    int bytes =
        headers.entrySet().stream()
            .mapToInt(entry -> utf8Length(entry.getKey()) + utf8Length(entry.getValue()))
            .sum();
    if (bytes > MAX_HEADER_BYTES) {
      throw new IllegalArgumentException("Job headers must not exceed 4 KiB in total");
    }
  }

  private static int utf8Length(String value) {
    return Objects.requireNonNull(value, "header value").getBytes(StandardCharsets.UTF_8).length;
  }

  private static String firstNonBlank(String preferred, String fallback) {
    if (preferred != null && !preferred.isBlank()) {
      return preferred;
    }
    return fallback;
  }

  private static String emptyToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
