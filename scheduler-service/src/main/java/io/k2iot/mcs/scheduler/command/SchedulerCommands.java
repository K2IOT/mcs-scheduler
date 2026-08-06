package io.k2iot.mcs.scheduler.command;

import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerSpec;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public final class SchedulerCommands {

  private SchedulerCommands() {}

  public record JobDraft(
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
      boolean durable) {

    public JobDraft {
      Objects.requireNonNull(jobId, "jobId");
      namespace = requireText(namespace, "namespace");
      name = requireText(name, "name");
      Objects.requireNonNull(destinationId, "destinationId");
      eventType = requireText(eventType, "eventType");
      payload = Map.copyOf(Objects.requireNonNull(payload, "payload"));
      headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
      Objects.requireNonNull(concurrencyPolicy, "concurrencyPolicy");
      Objects.requireNonNull(recoveryPolicy, "recoveryPolicy");
    }
  }

  public record TriggerDraft(
      UUID triggerId,
      UUID jobId,
      String namespace,
      String name,
      String description,
      TriggerSpec spec,
      Instant startAt,
      Instant endAt,
      int priority,
      String timezone,
      TriggerDefinition.MisfirePolicy misfirePolicy,
      Set<String> calendarNames) {

    public TriggerDraft {
      Objects.requireNonNull(triggerId, "triggerId");
      Objects.requireNonNull(jobId, "jobId");
      namespace = requireText(namespace, "namespace");
      name = requireText(name, "name");
      Objects.requireNonNull(spec, "spec");
      Objects.requireNonNull(misfirePolicy, "misfirePolicy");
      calendarNames = Set.copyOf(Objects.requireNonNull(calendarNames, "calendarNames"));
    }
  }

  public record CreateJob(UUID requestId, JobDraft job, String actor) {
    public CreateJob {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(job, "job");
      actor = requireText(actor, "actor");
    }
  }

  public record UpdateJob(UUID requestId, long expectedRevision, JobDraft job, String actor) {
    public UpdateJob {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(job, "job");
      actor = requireText(actor, "actor");
    }
  }

  public record CreateTrigger(UUID requestId, TriggerDraft trigger, String actor) {
    public CreateTrigger {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(trigger, "trigger");
      actor = requireText(actor, "actor");
    }
  }

  public record ReplaceTrigger(
      UUID requestId, long expectedRevision, TriggerDraft trigger, String actor) {
    public ReplaceTrigger {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(trigger, "trigger");
      actor = requireText(actor, "actor");
    }
  }

  public record CreateSchedule(
      UUID requestId, JobDraft job, List<TriggerDraft> triggers, String actor) {
    public CreateSchedule {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(job, "job");
      triggers = List.copyOf(Objects.requireNonNull(triggers, "triggers"));
      if (triggers.isEmpty()) {
        throw new IllegalArgumentException("triggers must not be empty");
      }
      actor = requireText(actor, "actor");
    }
  }

  public record JobMutation(
      UUID requestId, UUID jobId, String namespace, long expectedRevision, String actor) {
    public JobMutation {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(jobId, "jobId");
      namespace = requireText(namespace, "namespace");
      actor = requireText(actor, "actor");
    }
  }

  public record TriggerMutation(
      UUID requestId, UUID triggerId, String namespace, long expectedRevision, String actor) {
    public TriggerMutation {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(triggerId, "triggerId");
      namespace = requireText(namespace, "namespace");
      actor = requireText(actor, "actor");
    }
  }

  public record FireTriggerNow(
      UUID requestId,
      UUID triggerId,
      String namespace,
      UUID manualFireId,
      String actor) {
    public FireTriggerNow {
      Objects.requireNonNull(requestId, "requestId");
      Objects.requireNonNull(triggerId, "triggerId");
      namespace = requireText(namespace, "namespace");
      Objects.requireNonNull(manualFireId, "manualFireId");
      actor = requireText(actor, "actor");
    }
  }

  public record ScheduleResult(JobDefinition job, List<TriggerDefinition> triggers) {
    public ScheduleResult {
      Objects.requireNonNull(job, "job");
      triggers = List.copyOf(Objects.requireNonNull(triggers, "triggers"));
    }
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
