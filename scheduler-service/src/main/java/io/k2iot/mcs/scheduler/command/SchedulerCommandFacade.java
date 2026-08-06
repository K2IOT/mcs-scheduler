package io.k2iot.mcs.scheduler.command;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerValidator;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class SchedulerCommandFacade {

  private final JobRepository jobRepository;
  private final TriggerRepository triggerRepository;
  private final DestinationRepository destinationRepository;
  private final CommandRequestRepository commandRequestRepository;
  private final SchedulerProjectionPort schedulerProjection;
  private final JsonMapper jsonMapper;
  private final Clock clock;
  private final TriggerValidator triggerValidator;

  public SchedulerCommandFacade(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      CommandRequestRepository commandRequestRepository,
      SchedulerProjectionPort schedulerProjection,
      JsonMapper jsonMapper,
      Clock clock) {
    this(
        jobRepository,
        triggerRepository,
        destinationRepository,
        commandRequestRepository,
        schedulerProjection,
        jsonMapper,
        clock,
        new TriggerValidator());
  }

  public SchedulerCommandFacade(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      CommandRequestRepository commandRequestRepository,
      SchedulerProjectionPort schedulerProjection,
      JsonMapper jsonMapper,
      Clock clock,
      TriggerValidator triggerValidator) {
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
    this.triggerRepository = Objects.requireNonNull(triggerRepository, "triggerRepository");
    this.destinationRepository =
        Objects.requireNonNull(destinationRepository, "destinationRepository");
    this.commandRequestRepository =
        Objects.requireNonNull(commandRequestRepository, "commandRequestRepository");
    this.schedulerProjection = Objects.requireNonNull(schedulerProjection, "schedulerProjection");
    this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.triggerValidator = Objects.requireNonNull(triggerValidator, "triggerValidator");
  }

  @Transactional
  public JobDefinition createJob(SchedulerCommands.CreateJob command) {
    SchedulerCommands.JobDraft draft = command.job();
    return executeIdempotent(
        command.requestId(),
        "CREATE_JOB",
        draft.namespace(),
        draft.jobId(),
        command,
        () -> requireJob(draft.jobId()),
        () -> {
          validateDestination(draft);
          JobDefinition definition = newJob(draft, command.actor(), clock.instant());
          jobRepository.insert(definition);
          schedulerProjection.createJob(definition);
          return definition;
        });
  }

  @Transactional
  public JobDefinition updateJob(SchedulerCommands.UpdateJob command) {
    SchedulerCommands.JobDraft draft = command.job();
    return executeIdempotent(
        command.requestId(),
        "UPDATE_JOB",
        draft.namespace(),
        draft.jobId(),
        command,
        () -> requireJob(draft.jobId()),
        () -> {
          JobDefinition current = requireJob(draft.jobId());
          ensureNamespace(current.namespace(), draft.namespace());
          ensureRevision(current.revision(), command.expectedRevision());
          ensureNotDeleted(current.state(), "job");
          validateDestination(draft);
          JobDefinition updated =
              new JobDefinition(
                  current.jobId(),
                  draft.namespace(),
                  draft.name(),
                  draft.description(),
                  draft.destinationId(),
                  draft.destinationVersion(),
                  draft.eventType(),
                  draft.payload(),
                  draft.headers(),
                  draft.concurrencyPolicy(),
                  draft.recoveryPolicy(),
                  draft.durable(),
                  current.state(),
                  current.revision() + 1,
                  current.createdAt(),
                  current.createdBy(),
                  clock.instant(),
                  command.actor());
          updateJobOrThrow(updated, command.expectedRevision());
          schedulerProjection.updateJob(updated);
          return updated;
        });
  }

  @Transactional
  public TriggerDefinition createTrigger(SchedulerCommands.CreateTrigger command) {
    SchedulerCommands.TriggerDraft draft = command.trigger();
    return executeIdempotent(
        command.requestId(),
        "CREATE_TRIGGER",
        draft.namespace(),
        draft.triggerId(),
        command,
        () -> requireTrigger(draft.triggerId()),
        () -> createTriggerInternal(draft, command.actor(), clock.instant()));
  }

  @Transactional
  public TriggerDefinition replaceTrigger(SchedulerCommands.ReplaceTrigger command) {
    SchedulerCommands.TriggerDraft draft = command.trigger();
    return executeIdempotent(
        command.requestId(),
        "REPLACE_TRIGGER",
        draft.namespace(),
        draft.triggerId(),
        command,
        () -> requireTrigger(draft.triggerId()),
        () -> {
          TriggerDefinition current = requireTrigger(draft.triggerId());
          ensureNamespace(current.namespace(), draft.namespace());
          ensureRevision(current.revision(), command.expectedRevision());
          ensureNotDeleted(current.state(), "trigger");
          JobDefinition job = requireJob(draft.jobId());
          ensureNamespace(job.namespace(), draft.namespace());
          TriggerDefinition replacement =
              new TriggerDefinition(
                  current.triggerId(),
                  draft.jobId(),
                  draft.namespace(),
                  draft.name(),
                  draft.description(),
                  draft.spec(),
                  draft.startAt(),
                  draft.endAt(),
                  draft.priority(),
                  draft.timezone(),
                  draft.misfirePolicy(),
                  draft.calendarNames(),
                  current.state(),
                  current.revision() + 1,
                  current.createdAt(),
                  current.createdBy(),
                  clock.instant(),
                  command.actor());
          triggerValidator.validate(replacement, clock.instant());
          updateTriggerOrThrow(replacement, command.expectedRevision());
          schedulerProjection.replaceTrigger(replacement);
          return replacement;
        });
  }

  @Transactional
  public SchedulerCommands.ScheduleResult createSchedule(SchedulerCommands.CreateSchedule command) {
    SchedulerCommands.JobDraft jobDraft = command.job();
    return executeIdempotent(
        command.requestId(),
        "CREATE_SCHEDULE",
        jobDraft.namespace(),
        jobDraft.jobId(),
        command,
        () -> replaySchedule(command),
        () -> {
          Instant now = clock.instant();
          validateDestination(jobDraft);
          JobDefinition job = newJob(jobDraft, command.actor(), now);
          jobRepository.insert(job);
          schedulerProjection.createJob(job);

          List<TriggerDefinition> triggers = new ArrayList<>();
          for (SchedulerCommands.TriggerDraft triggerDraft : command.triggers()) {
            if (!triggerDraft.jobId().equals(job.jobId())) {
              throw new SchedulerCommandException(
                  "TRIGGER_JOB_MISMATCH", "Every schedule trigger must reference the created job");
            }
            ensureNamespace(job.namespace(), triggerDraft.namespace());
            TriggerDefinition trigger = newTrigger(triggerDraft, command.actor(), now);
            triggerValidator.validate(trigger, now);
            triggerRepository.insert(trigger);
            schedulerProjection.createTrigger(trigger);
            triggers.add(trigger);
          }
          return new SchedulerCommands.ScheduleResult(job, triggers);
        });
  }

  @Transactional
  public JobDefinition pauseJob(SchedulerCommands.JobMutation command) {
    return mutateJob(
        command, "PAUSE_JOB", JobDefinition.State.PAUSED, schedulerProjection::pauseJob);
  }

  @Transactional
  public JobDefinition resumeJob(SchedulerCommands.JobMutation command) {
    return mutateJob(
        command, "RESUME_JOB", JobDefinition.State.ACTIVE, schedulerProjection::resumeJob);
  }

  @Transactional
  public JobDefinition deleteJob(SchedulerCommands.JobMutation command) {
    return mutateJob(
        command, "DELETE_JOB", JobDefinition.State.DELETED, schedulerProjection::deleteJob);
  }

  @Transactional
  public TriggerDefinition pauseTrigger(SchedulerCommands.TriggerMutation command) {
    return mutateTrigger(
        command,
        "PAUSE_TRIGGER",
        TriggerDefinition.State.PAUSED,
        schedulerProjection::pauseTrigger);
  }

  @Transactional
  public TriggerDefinition resumeTrigger(SchedulerCommands.TriggerMutation command) {
    return mutateTrigger(
        command,
        "RESUME_TRIGGER",
        TriggerDefinition.State.ACTIVE,
        schedulerProjection::resumeTrigger);
  }

  @Transactional
  public TriggerDefinition deleteTrigger(SchedulerCommands.TriggerMutation command) {
    return mutateTrigger(
        command,
        "DELETE_TRIGGER",
        TriggerDefinition.State.DELETED,
        schedulerProjection::deleteTrigger);
  }

  @Transactional
  public ManualFireResult fireTriggerNow(SchedulerCommands.FireTriggerNow command) {
    return executeIdempotent(
        command.requestId(),
        "FIRE_TRIGGER_NOW",
        command.namespace(),
        command.triggerId(),
        command,
        () -> {
          TriggerDefinition trigger = requireTrigger(command.triggerId());
          return new ManualFireResult(command.manualFireId(), trigger.triggerId(), trigger.jobId());
        },
        () -> {
          TriggerDefinition trigger = requireTrigger(command.triggerId());
          ensureNamespace(trigger.namespace(), command.namespace());
          ensureNotDeleted(trigger.state(), "trigger");
          schedulerProjection.fireTriggerNow(trigger, command.manualFireId());
          return new ManualFireResult(command.manualFireId(), trigger.triggerId(), trigger.jobId());
        });
  }

  private TriggerDefinition createTriggerInternal(
      SchedulerCommands.TriggerDraft draft, String actor, Instant now) {
    JobDefinition job = requireJob(draft.jobId());
    ensureNamespace(job.namespace(), draft.namespace());
    ensureNotDeleted(job.state(), "job");
    TriggerDefinition definition = newTrigger(draft, actor, now);
    triggerValidator.validate(definition, now);
    triggerRepository.insert(definition);
    schedulerProjection.createTrigger(definition);
    return definition;
  }

  private JobDefinition mutateJob(
      SchedulerCommands.JobMutation command,
      String commandType,
      JobDefinition.State targetState,
      Consumer<JobDefinition> projectionMutation) {
    return executeIdempotent(
        command.requestId(),
        commandType,
        command.namespace(),
        command.jobId(),
        command,
        () -> requireJob(command.jobId()),
        () -> {
          JobDefinition current = requireJob(command.jobId());
          ensureNamespace(current.namespace(), command.namespace());
          ensureRevision(current.revision(), command.expectedRevision());
          ensureNotDeleted(current.state(), "job");
          JobDefinition updated =
              new JobDefinition(
                  current.jobId(),
                  current.namespace(),
                  current.name(),
                  current.description(),
                  current.destinationId(),
                  current.destinationVersion(),
                  current.eventType(),
                  current.payload(),
                  current.headers(),
                  current.concurrencyPolicy(),
                  current.recoveryPolicy(),
                  current.durable(),
                  targetState,
                  current.revision() + 1,
                  current.createdAt(),
                  current.createdBy(),
                  clock.instant(),
                  command.actor());
          updateJobOrThrow(updated, command.expectedRevision());
          projectionMutation.accept(updated);
          return updated;
        });
  }

  private TriggerDefinition mutateTrigger(
      SchedulerCommands.TriggerMutation command,
      String commandType,
      TriggerDefinition.State targetState,
      Consumer<TriggerDefinition> projectionMutation) {
    return executeIdempotent(
        command.requestId(),
        commandType,
        command.namespace(),
        command.triggerId(),
        command,
        () -> requireTrigger(command.triggerId()),
        () -> {
          TriggerDefinition current = requireTrigger(command.triggerId());
          ensureNamespace(current.namespace(), command.namespace());
          ensureRevision(current.revision(), command.expectedRevision());
          ensureNotDeleted(current.state(), "trigger");
          TriggerDefinition updated =
              new TriggerDefinition(
                  current.triggerId(),
                  current.jobId(),
                  current.namespace(),
                  current.name(),
                  current.description(),
                  current.spec(),
                  current.startAt(),
                  current.endAt(),
                  current.priority(),
                  current.timezone(),
                  current.misfirePolicy(),
                  current.calendarNames(),
                  targetState,
                  current.revision() + 1,
                  current.createdAt(),
                  current.createdBy(),
                  clock.instant(),
                  command.actor());
          updateTriggerOrThrow(updated, command.expectedRevision());
          projectionMutation.accept(updated);
          return updated;
        });
  }

  private <T> T executeIdempotent(
      UUID requestId,
      String commandType,
      String namespace,
      UUID aggregateId,
      Object commandPayload,
      Supplier<T> replay,
      Supplier<T> action) {
    JsonNode payload = jsonMapper.valueToTree(commandPayload);
    String requestHash = RequestFingerprint.sha256(jsonMapper, commandPayload);
    var existing = commandRequestRepository.findByRequestId(requestId);
    if (existing.isPresent()) {
      return resolveExistingRequest(existing.orElseThrow(), requestHash, commandType, replay);
    }

    Instant requestedAt = clock.instant();
    CommandRequest processing =
        CommandRequest.processing(
            UUID.randomUUID(),
            requestId,
            commandType,
            namespace,
            aggregateId,
            requestHash,
            payload,
            requestedAt);
    if (!commandRequestRepository.insertIfAbsent(processing)) {
      CommandRequest winner =
          commandRequestRepository
              .findByRequestId(requestId)
              .orElseThrow(
                  () ->
                      new SchedulerCommandException(
                          "IDEMPOTENCY_CLAIM_LOST",
                          "Request ID was claimed but no command request is visible"));
      return resolveExistingRequest(winner, requestHash, commandType, replay);
    }

    T result = action.get();
    commandRequestRepository.complete(requestId, jsonMapper.valueToTree(result), clock.instant());
    return result;
  }

  private <T> T resolveExistingRequest(
      CommandRequest request, String requestHash, String commandType, Supplier<T> replay) {
    if (!request.requestHash().equals(requestHash)) {
      throw new SchedulerCommandException(
          "IDEMPOTENCY_CONFLICT", "Request ID was already used with a different payload");
    }
    return switch (request.status()) {
      case COMPLETED ->
          request.responseJson() == null
              ? replay.get()
              : decodeStoredResponse(commandType, request.responseJson());
      case RECEIVED, PROCESSING ->
          throw new SchedulerCommandException(
              "COMMAND_IN_PROGRESS", "The command with this request ID is still in progress");
      case FAILED ->
          throw new SchedulerCommandException(
              "COMMAND_PREVIOUSLY_FAILED", "The command with this request ID previously failed");
    };
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeStoredResponse(String commandType, JsonNode responseJson) {
    Class<?> responseType =
        switch (commandType) {
          case "CREATE_JOB", "UPDATE_JOB", "PAUSE_JOB", "RESUME_JOB", "DELETE_JOB" ->
              JobDefinition.class;
          case "CREATE_TRIGGER",
              "REPLACE_TRIGGER",
              "PAUSE_TRIGGER",
              "RESUME_TRIGGER",
              "DELETE_TRIGGER" ->
              TriggerDefinition.class;
          case "CREATE_SCHEDULE" -> SchedulerCommands.ScheduleResult.class;
          case "FIRE_TRIGGER_NOW" -> ManualFireResult.class;
          default ->
              throw new SchedulerCommandException(
                  "UNSUPPORTED_COMMAND_RESPONSE",
                  "No stored response mapping exists for command type " + commandType);
        };
    try {
      return (T) jsonMapper.treeToValue(responseJson, responseType);
    } catch (JacksonException exception) {
      throw new SchedulerCommandException(
          "INVALID_STORED_RESPONSE",
          "Stored response cannot be decoded for command type " + commandType,
          exception);
    }
  }

  private SchedulerCommands.ScheduleResult replaySchedule(
      SchedulerCommands.CreateSchedule command) {
    JobDefinition job = requireJob(command.job().jobId());
    List<TriggerDefinition> triggers =
        command.triggers().stream()
            .map(SchedulerCommands.TriggerDraft::triggerId)
            .map(this::requireTrigger)
            .toList();
    return new SchedulerCommands.ScheduleResult(job, triggers);
  }

  private void validateDestination(SchedulerCommands.JobDraft draft) {
    DestinationDefinition destination =
        destinationRepository
            .findByIdAndVersion(draft.destinationId(), draft.destinationVersion())
            .orElseThrow(
                () ->
                    new SchedulerCommandException(
                        "DESTINATION_NOT_FOUND", "Destination version does not exist"));
    ensureNamespace(destination.namespace(), draft.namespace());
    if (!destination.enabled()) {
      throw new SchedulerCommandException(
          "DESTINATION_DISABLED", "Destination version is disabled");
    }
  }

  private JobDefinition newJob(SchedulerCommands.JobDraft draft, String actor, Instant now) {
    return new JobDefinition(
        draft.jobId(),
        draft.namespace(),
        draft.name(),
        draft.description(),
        draft.destinationId(),
        draft.destinationVersion(),
        draft.eventType(),
        draft.payload(),
        draft.headers(),
        draft.concurrencyPolicy(),
        draft.recoveryPolicy(),
        draft.durable(),
        JobDefinition.State.ACTIVE,
        1,
        now,
        actor,
        now,
        actor);
  }

  private TriggerDefinition newTrigger(
      SchedulerCommands.TriggerDraft draft, String actor, Instant now) {
    return new TriggerDefinition(
        draft.triggerId(),
        draft.jobId(),
        draft.namespace(),
        draft.name(),
        draft.description(),
        draft.spec(),
        draft.startAt(),
        draft.endAt(),
        draft.priority(),
        draft.timezone(),
        draft.misfirePolicy(),
        draft.calendarNames(),
        TriggerDefinition.State.ACTIVE,
        1,
        now,
        actor,
        now,
        actor);
  }

  private JobDefinition requireJob(UUID jobId) {
    return jobRepository
        .findById(jobId)
        .orElseThrow(
            () -> new SchedulerCommandException("JOB_NOT_FOUND", "Job does not exist: " + jobId));
  }

  private TriggerDefinition requireTrigger(UUID triggerId) {
    return triggerRepository
        .findById(triggerId)
        .orElseThrow(
            () ->
                new SchedulerCommandException(
                    "TRIGGER_NOT_FOUND", "Trigger does not exist: " + triggerId));
  }

  private void updateJobOrThrow(JobDefinition definition, long expectedRevision) {
    if (!jobRepository.update(definition, expectedRevision)) {
      throw new SchedulerCommandException(
          "REVISION_CONFLICT", "Job revision changed while applying the command");
    }
  }

  private void updateTriggerOrThrow(TriggerDefinition definition, long expectedRevision) {
    if (!triggerRepository.update(definition, expectedRevision)) {
      throw new SchedulerCommandException(
          "REVISION_CONFLICT", "Trigger revision changed while applying the command");
    }
  }

  private void ensureRevision(long actualRevision, long expectedRevision) {
    if (actualRevision != expectedRevision) {
      throw new SchedulerCommandException(
          "REVISION_CONFLICT",
          "Expected revision " + expectedRevision + " but found " + actualRevision);
    }
  }

  private void ensureNamespace(String actual, String expected) {
    if (!actual.equals(expected)) {
      throw new SchedulerCommandException(
          "NAMESPACE_MISMATCH", "Resource belongs to a different namespace");
    }
  }

  private void ensureNotDeleted(Object state, String resourceType) {
    if (state == JobDefinition.State.DELETED || state == TriggerDefinition.State.DELETED) {
      throw new SchedulerCommandException(
          "RESOURCE_DELETED", "The " + resourceType + " has already been deleted");
    }
  }
}
