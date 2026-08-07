package io.k2iot.mcs.scheduler.command;

import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

public final class LifecycleSchedulerCommandFacade extends SchedulerCommandFacade {

  private final JobRepository jobRepository;
  private final TriggerRepository triggerRepository;
  private final CommandRequestRepository commandRequestRepository;
  private final SchedulerProjectionPort schedulerProjection;
  private final AuditRepository auditRepository;
  private final Clock clock;

  public LifecycleSchedulerCommandFacade(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      CommandRequestRepository commandRequestRepository,
      SchedulerProjectionPort schedulerProjection,
      JsonMapper jsonMapper,
      Clock clock,
      AuditRepository auditRepository) {
    super(
        jobRepository,
        triggerRepository,
        destinationRepository,
        commandRequestRepository,
        schedulerProjection,
        jsonMapper,
        clock);
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
    this.triggerRepository = Objects.requireNonNull(triggerRepository, "triggerRepository");
    this.commandRequestRepository =
        Objects.requireNonNull(commandRequestRepository, "commandRequestRepository");
    this.schedulerProjection = Objects.requireNonNull(schedulerProjection, "schedulerProjection");
    this.auditRepository = Objects.requireNonNull(auditRepository, "auditRepository");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  @Transactional
  public JobDefinition createJob(SchedulerCommands.CreateJob command) {
    boolean replay = isReplay(command.requestId());
    JobDefinition result = super.createJob(command);
    if (!replay) {
      audit(
          command.requestId(),
          "JOB",
          result.jobId(),
          "CREATE_JOB",
          command.actor(),
          null,
          null,
          result.revision(),
          result.state().name(),
          Map.of());
    }
    return result;
  }

  @Override
  @Transactional
  public JobDefinition updateJob(SchedulerCommands.UpdateJob command) {
    boolean replay = isReplay(command.requestId());
    JobDefinition before =
        replay ? null : jobRepository.findById(command.job().jobId()).orElse(null);
    JobDefinition result = super.updateJob(command);
    if (!replay) {
      auditJob(command.requestId(), "UPDATE_JOB", command.actor(), before, result, Map.of());
    }
    return result;
  }

  @Override
  @Transactional
  public TriggerDefinition createTrigger(SchedulerCommands.CreateTrigger command) {
    boolean replay = isReplay(command.requestId());
    TriggerDefinition result = super.createTrigger(command);
    if (!replay) {
      audit(
          command.requestId(),
          "TRIGGER",
          result.triggerId(),
          "CREATE_TRIGGER",
          command.actor(),
          null,
          null,
          result.revision(),
          result.state().name(),
          Map.of("jobId", result.jobId().toString()));
    }
    return result;
  }

  @Override
  @Transactional
  public TriggerDefinition replaceTrigger(SchedulerCommands.ReplaceTrigger command) {
    boolean replay = isReplay(command.requestId());
    TriggerDefinition before =
        replay ? null : triggerRepository.findById(command.trigger().triggerId()).orElse(null);
    TriggerDefinition result = super.replaceTrigger(command);
    if (!replay) {
      auditTrigger(
          command.requestId(), "REPLACE_TRIGGER", command.actor(), before, result, Map.of());
    }
    return result;
  }

  @Override
  @Transactional
  public SchedulerCommands.ScheduleResult createSchedule(SchedulerCommands.CreateSchedule command) {
    boolean replay = isReplay(command.requestId());
    SchedulerCommands.ScheduleResult result = super.createSchedule(command);
    if (!replay) {
      JobDefinition job = result.job();
      audit(
          command.requestId(),
          "JOB",
          job.jobId(),
          "CREATE_JOB",
          command.actor(),
          null,
          null,
          job.revision(),
          job.state().name(),
          Map.of("via", "CREATE_SCHEDULE"));
      for (TriggerDefinition trigger : result.triggers()) {
        audit(
            command.requestId(),
            "TRIGGER",
            trigger.triggerId(),
            "CREATE_TRIGGER",
            command.actor(),
            null,
            null,
            trigger.revision(),
            trigger.state().name(),
            Map.of("jobId", trigger.jobId().toString(), "via", "CREATE_SCHEDULE"));
      }
    }
    return result;
  }

  @Override
  @Transactional
  public JobDefinition pauseJob(SchedulerCommands.JobMutation command) {
    if (isReplay(command.requestId())) {
      return super.pauseJob(command);
    }
    JobDefinition before = jobRepository.findById(command.jobId()).orElse(null);
    List<TriggerDefinition> activeTriggers =
        triggerRepository.findByJobId(command.jobId()).stream()
            .filter(trigger -> trigger.state() == TriggerDefinition.State.ACTIVE)
            .toList();

    JobDefinition result = super.pauseJob(command);
    auditJob(command.requestId(), "PAUSE_JOB", command.actor(), before, result, Map.of());

    for (TriggerDefinition trigger : activeTriggers) {
      TriggerDefinition paused =
          copyTrigger(trigger, TriggerDefinition.State.PAUSED, command.actor(), clock.instant());
      updateTrigger(paused, trigger.revision());
      triggerRepository.setPauseReason(trigger.triggerId(), TriggerRepository.PauseReason.JOB);
      auditTrigger(
          command.requestId(),
          "PAUSE_TRIGGER",
          command.actor(),
          trigger,
          paused,
          Map.of("pauseReason", "JOB", "jobId", command.jobId().toString()));
    }
    return result;
  }

  @Override
  @Transactional
  public JobDefinition resumeJob(SchedulerCommands.JobMutation command) {
    if (isReplay(command.requestId())) {
      return super.resumeJob(command);
    }
    JobDefinition before = jobRepository.findById(command.jobId()).orElse(null);
    List<TriggerDefinition> pausedByJob =
        triggerRepository.findPausedByJobId(command.jobId(), TriggerRepository.PauseReason.JOB);
    List<TriggerDefinition> individuallyPaused =
        triggerRepository.findPausedByJobId(
            command.jobId(), TriggerRepository.PauseReason.INDIVIDUAL);

    JobDefinition result = super.resumeJob(command);
    auditJob(command.requestId(), "RESUME_JOB", command.actor(), before, result, Map.of());

    for (TriggerDefinition trigger : pausedByJob) {
      TriggerDefinition resumed =
          copyTrigger(trigger, TriggerDefinition.State.ACTIVE, command.actor(), clock.instant());
      updateTrigger(resumed, trigger.revision());
      schedulerProjection.resumeTrigger(resumed);
      auditTrigger(
          command.requestId(),
          "RESUME_TRIGGER",
          command.actor(),
          trigger,
          resumed,
          Map.of("pauseReason", "JOB", "jobId", command.jobId().toString()));
    }

    for (TriggerDefinition trigger : individuallyPaused) {
      schedulerProjection.pauseTrigger(trigger);
    }
    return result;
  }

  @Override
  @Transactional
  public JobDefinition deleteJob(SchedulerCommands.JobMutation command) {
    boolean replay = isReplay(command.requestId());
    JobDefinition before = replay ? null : jobRepository.findById(command.jobId()).orElse(null);
    JobDefinition result = super.deleteJob(command);
    if (!replay) {
      auditJob(command.requestId(), "DELETE_JOB", command.actor(), before, result, Map.of());
    }
    return result;
  }

  @Override
  @Transactional
  public TriggerDefinition pauseTrigger(SchedulerCommands.TriggerMutation command) {
    if (isReplay(command.requestId())) {
      return super.pauseTrigger(command);
    }
    TriggerDefinition before = triggerRepository.findById(command.triggerId()).orElse(null);
    TriggerDefinition result = super.pauseTrigger(command);
    triggerRepository.setPauseReason(command.triggerId(), TriggerRepository.PauseReason.INDIVIDUAL);
    auditTrigger(
        command.requestId(),
        "PAUSE_TRIGGER",
        command.actor(),
        before,
        result,
        Map.of("pauseReason", "INDIVIDUAL"));
    return result;
  }

  @Override
  @Transactional
  public TriggerDefinition resumeTrigger(SchedulerCommands.TriggerMutation command) {
    boolean replay = isReplay(command.requestId());
    TriggerDefinition before =
        replay ? null : triggerRepository.findById(command.triggerId()).orElse(null);
    TriggerDefinition result = super.resumeTrigger(command);
    if (!replay) {
      auditTrigger(
          command.requestId(), "RESUME_TRIGGER", command.actor(), before, result, Map.of());
    }
    return result;
  }

  @Override
  @Transactional
  public TriggerDefinition deleteTrigger(SchedulerCommands.TriggerMutation command) {
    boolean replay = isReplay(command.requestId());
    TriggerDefinition before =
        replay ? null : triggerRepository.findById(command.triggerId()).orElse(null);
    TriggerDefinition result = super.deleteTrigger(command);
    if (!replay) {
      auditTrigger(
          command.requestId(), "DELETE_TRIGGER", command.actor(), before, result, Map.of());
    }
    return result;
  }

  @Override
  @Transactional
  public ManualFireResult fireTriggerNow(SchedulerCommands.FireTriggerNow command) {
    boolean replay = isReplay(command.requestId());
    TriggerDefinition trigger =
        replay ? null : triggerRepository.findById(command.triggerId()).orElse(null);
    ManualFireResult result = super.fireTriggerNow(command);
    if (!replay && trigger != null) {
      audit(
          command.requestId(),
          "TRIGGER",
          trigger.triggerId(),
          "FIRE_TRIGGER_NOW",
          command.actor(),
          trigger.revision(),
          trigger.state().name(),
          trigger.revision(),
          trigger.state().name(),
          Map.of("manualFireId", command.manualFireId().toString()));
    }
    return result;
  }

  private boolean isReplay(UUID requestId) {
    return commandRequestRepository.findByRequestId(requestId).isPresent();
  }

  private void updateTrigger(TriggerDefinition definition, long expectedRevision) {
    if (!triggerRepository.update(definition, expectedRevision)) {
      throw new SchedulerCommandException(
          "REVISION_CONFLICT", "Trigger revision changed while applying job lifecycle");
    }
  }

  private TriggerDefinition copyTrigger(
      TriggerDefinition current, TriggerDefinition.State state, String actor, Instant updatedAt) {
    return new TriggerDefinition(
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
        state,
        current.revision() + 1,
        current.createdAt(),
        current.createdBy(),
        updatedAt,
        actor);
  }

  private void auditJob(
      UUID requestId,
      String action,
      String actor,
      JobDefinition before,
      JobDefinition after,
      Map<String, Object> metadata) {
    audit(
        requestId,
        "JOB",
        after.jobId(),
        action,
        actor,
        before == null ? null : before.revision(),
        before == null ? null : before.state().name(),
        after.revision(),
        after.state().name(),
        metadata);
  }

  private void auditTrigger(
      UUID requestId,
      String action,
      String actor,
      TriggerDefinition before,
      TriggerDefinition after,
      Map<String, Object> metadata) {
    audit(
        requestId,
        "TRIGGER",
        after.triggerId(),
        action,
        actor,
        before == null ? null : before.revision(),
        before == null ? null : before.state().name(),
        after.revision(),
        after.state().name(),
        metadata);
  }

  private void audit(
      UUID requestId,
      String aggregateType,
      UUID aggregateId,
      String action,
      String actor,
      Long oldRevision,
      String oldState,
      Long newRevision,
      String newState,
      Map<String, Object> metadata) {
    auditRepository.append(
        new AuditRepository.AuditEvent(
            requestId,
            aggregateType,
            aggregateId,
            action,
            actor,
            oldRevision,
            oldState,
            newRevision,
            newState,
            clock.instant(),
            metadata));
  }
}
