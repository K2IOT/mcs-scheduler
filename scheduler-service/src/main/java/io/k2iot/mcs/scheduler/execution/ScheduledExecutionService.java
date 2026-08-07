package io.k2iot.mcs.scheduler.execution;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.outbox.OutboxRepository;
import io.k2iot.mcs.scheduler.quartz.QuartzKeys;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.springframework.transaction.annotation.Transactional;

public class ScheduledExecutionService {

  private final JobRepository jobRepository;
  private final TriggerRepository triggerRepository;
  private final DestinationRepository destinationRepository;
  private final ExecutionRepository executionRepository;
  private final OutboxRepository outboxRepository;
  private final ExecutionEventFactory eventFactory;
  private final Clock clock;

  public ScheduledExecutionService(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      ExecutionRepository executionRepository,
      OutboxRepository outboxRepository,
      ExecutionEventFactory eventFactory,
      Clock clock) {
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
    this.triggerRepository = Objects.requireNonNull(triggerRepository, "triggerRepository");
    this.destinationRepository =
        Objects.requireNonNull(destinationRepository, "destinationRepository");
    this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository");
    this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository");
    this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Transactional
  public void record(JobExecutionContext context) {
    JobDataMap data = context.getMergedJobDataMap();
    UUID jobId = requiredUuid(data, QuartzKeys.JOB_ID);
    UUID sourceTriggerId = triggerId(context, data);
    UUID manualFireId = optionalUuid(data, QuartzKeys.MANUAL_FIRE_ID);
    boolean manual = manualFireId != null;

    Instant scheduledFireTime = manual ? null : requiredFireTime(context.getScheduledFireTime());
    UUID executionId =
        manual
            ? ExecutionIdentity.forManual(manualFireId)
            : ExecutionIdentity.forScheduled(sourceTriggerId, scheduledFireTime);
    Instant actualFireTime = fireTimeOrNow(context.getFireTime());

    JobDefinition job =
        jobRepository
            .findById(jobId)
            .orElseThrow(() -> new IllegalStateException("Job definition not found: " + jobId));
    TriggerDefinition trigger =
        triggerRepository
            .findById(sourceTriggerId)
            .orElseThrow(
                () -> new IllegalStateException("Trigger definition not found: " + sourceTriggerId));
    DestinationDefinition destination =
        destinationRepository
            .findByIdAndVersion(job.destinationId(), job.destinationVersion())
            .orElse(null);

    boolean suppressed = shouldSuppress(job, trigger, destination, sourceTriggerId);
    Map<String, Object> snapshot = snapshot(job, trigger, destination, context.isRecovering());
    Instant now = clock.instant();

    ExecutionRepository.ExecutionRecord execution =
        new ExecutionRepository.ExecutionRecord(
            executionId,
            jobId,
            manual ? null : sourceTriggerId,
            manualFireId,
            scheduledFireTime,
            actualFireTime,
            suppressed ? ExecutionRepository.Status.SUPPRESSED : ExecutionRepository.Status.SCHEDULED,
            1,
            snapshot,
            now,
            now);

    if (!executionRepository.insertIfAbsent(execution) || suppressed) {
      return;
    }

    outboxRepository.insert(
        eventFactory.create(
            executionId,
            jobId,
            sourceTriggerId,
            job.namespace(),
            job.eventType(),
            scheduledFireTime,
            actualFireTime,
            context.isRecovering(),
            job,
            Objects.requireNonNull(destination)));
  }

  private boolean shouldSuppress(
      JobDefinition job,
      TriggerDefinition trigger,
      DestinationDefinition destination,
      UUID sourceTriggerId) {
    return job.state() != JobDefinition.State.ACTIVE
        || trigger.state() != TriggerDefinition.State.ACTIVE
        || !trigger.triggerId().equals(sourceTriggerId)
        || !trigger.jobId().equals(job.jobId())
        || !trigger.namespace().equals(job.namespace())
        || destination == null
        || !destination.enabled()
        || !destination.destinationId().equals(job.destinationId())
        || destination.version() != job.destinationVersion()
        || !destination.namespace().equals(job.namespace());
  }

  private Map<String, Object> snapshot(
      JobDefinition job,
      TriggerDefinition trigger,
      DestinationDefinition destination,
      boolean recovery) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("namespace", job.namespace());
    snapshot.put("eventType", job.eventType());
    snapshot.put("jobRevision", job.revision());
    snapshot.put("triggerRevision", trigger.revision());
    snapshot.put("destinationId", job.destinationId().toString());
    snapshot.put("destinationVersion", job.destinationVersion());
    if (destination != null) {
      snapshot.put("destinationTopic", destination.topic());
    }
    snapshot.put("payload", job.payload());
    snapshot.put("headers", job.headers());
    snapshot.put("recovery", recovery);
    return Map.copyOf(snapshot);
  }

  private UUID triggerId(JobExecutionContext context, JobDataMap data) {
    UUID fromData = optionalUuid(data, QuartzKeys.TRIGGER_ID);
    if (fromData != null) {
      return fromData;
    }
    if (context.getTrigger() == null || context.getTrigger().getKey() == null) {
      throw new IllegalStateException("Quartz trigger identity is missing");
    }
    try {
      return UUID.fromString(context.getTrigger().getKey().getName());
    } catch (IllegalArgumentException exception) {
      throw new IllegalStateException(
          "Quartz trigger name is not a scheduler trigger UUID: "
              + context.getTrigger().getKey().getName(),
          exception);
    }
  }

  private static UUID requiredUuid(JobDataMap data, String key) {
    UUID value = optionalUuid(data, key);
    if (value == null) {
      throw new IllegalStateException("Missing Quartz JobDataMap UUID: " + key);
    }
    return value;
  }

  private static UUID optionalUuid(JobDataMap data, String key) {
    String raw = data.getString(key);
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return UUID.fromString(raw);
    } catch (IllegalArgumentException exception) {
      throw new IllegalStateException("Invalid Quartz JobDataMap UUID for " + key + ": " + raw, exception);
    }
  }

  private Instant requiredFireTime(Date scheduledFireTime) {
    if (scheduledFireTime == null) {
      throw new IllegalStateException("Quartz scheduled fire time is missing");
    }
    return scheduledFireTime.toInstant();
  }

  private Instant fireTimeOrNow(Date fireTime) {
    return fireTime == null ? clock.instant() : fireTime.toInstant();
  }
}
