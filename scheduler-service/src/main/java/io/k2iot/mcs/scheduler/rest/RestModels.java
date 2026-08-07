package io.k2iot.mcs.scheduler.rest;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class RestModels {

  private RestModels() {}

  public record JobRequest(
      UUID jobId,
      String namespace,
      String name,
      String description,
      UUID destinationId,
      long destinationVersion,
      String eventType,
      Map<String, Object> payload,
      Map<String, String> headers,
      ConcurrencyPolicy concurrencyPolicy,
      RecoveryPolicy recoveryPolicy,
      boolean durable) {}

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = OnceTriggerRequest.class, name = "ONCE"),
    @JsonSubTypes.Type(value = CronTriggerRequest.class, name = "CRON"),
    @JsonSubTypes.Type(value = SimpleIntervalTriggerRequest.class, name = "SIMPLE_INTERVAL"),
    @JsonSubTypes.Type(value = CalendarIntervalTriggerRequest.class, name = "CALENDAR_INTERVAL"),
    @JsonSubTypes.Type(value = DailyTimeIntervalTriggerRequest.class, name = "DAILY_TIME_INTERVAL")
  })
  public sealed interface TriggerRequest
      permits OnceTriggerRequest,
          CronTriggerRequest,
          SimpleIntervalTriggerRequest,
          CalendarIntervalTriggerRequest,
          DailyTimeIntervalTriggerRequest {}

  public record OnceTriggerRequest(Instant fireAt) implements TriggerRequest {}

  public record CronTriggerRequest(String expression) implements TriggerRequest {}

  public record SimpleIntervalTriggerRequest(Duration interval, Long repeatCount)
      implements TriggerRequest {}

  public record CalendarIntervalTriggerRequest(int interval, ChronoUnit unit)
      implements TriggerRequest {}

  public record DailyTimeIntervalTriggerRequest(
      int interval,
      ChronoUnit unit,
      Set<DayOfWeek> daysOfWeek,
      LocalTime startTime,
      LocalTime endTime)
      implements TriggerRequest {}

  public record TriggerDraftRequest(
      UUID triggerId,
      UUID jobId,
      String namespace,
      String name,
      String description,
      TriggerRequest spec,
      Instant startAt,
      Instant endAt,
      int priority,
      String timezone,
      TriggerDefinition.MisfirePolicy misfirePolicy,
      Set<String> calendarNames) {}

  public record CreateJobRequest(String caller, JobRequest job) {}

  public record UpdateJobRequest(String caller, JobRequest job) {}

  public record CreateTriggerRequest(String caller, TriggerDraftRequest trigger) {}

  public record ReplaceTriggerRequest(String caller, TriggerDraftRequest trigger) {}

  public record CreateScheduleRequest(
      String caller, JobRequest job, List<TriggerDraftRequest> triggers) {}

  public record MutationRequest(String namespace, String caller) {}

  public record FireTriggerRequest(
      String caller, String namespace, UUID triggerId, UUID manualFireId) {}

  public record JobResponse(JobDefinition job) {}

  public record TriggerResponse(TriggerDefinition trigger) {}

  public record ScheduleResponse(JobDefinition job, List<TriggerDefinition> triggers) {}

  public record ExecutionResponse(UUID manualFireId, UUID triggerId, UUID jobId) {}
}
