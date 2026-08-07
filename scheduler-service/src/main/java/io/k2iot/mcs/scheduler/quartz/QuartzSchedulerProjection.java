package io.k2iot.mcs.scheduler.quartz;

import io.k2iot.mcs.scheduler.command.SchedulerProjectionPort;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

public final class QuartzSchedulerProjection implements SchedulerProjectionPort {

  private final Supplier<Scheduler> schedulerSupplier;
  private final QuartzTriggerMapper triggerMapper;

  public QuartzSchedulerProjection(Scheduler scheduler, QuartzTriggerMapper triggerMapper) {
    this(() -> Objects.requireNonNull(scheduler, "scheduler"), triggerMapper);
  }

  public QuartzSchedulerProjection(
      Supplier<Scheduler> schedulerSupplier, QuartzTriggerMapper triggerMapper) {
    this.schedulerSupplier = Objects.requireNonNull(schedulerSupplier, "schedulerSupplier");
    this.triggerMapper = Objects.requireNonNull(triggerMapper, "triggerMapper");
  }

  @Override
  public void createJob(JobDefinition definition) {
    execute(
        "create job " + definition.jobId(),
        () -> scheduler().addJob(toJobDetail(definition), false, true));
  }

  @Override
  public void updateJob(JobDefinition definition) {
    execute(
        "update job " + definition.jobId(),
        () -> scheduler().addJob(toJobDetail(definition), true, true));
  }

  @Override
  public void pauseJob(JobDefinition definition) {
    execute("pause job " + definition.jobId(), () -> scheduler().pauseJob(jobKey(definition)));
  }

  @Override
  public void resumeJob(JobDefinition definition) {
    execute("resume job " + definition.jobId(), () -> scheduler().resumeJob(jobKey(definition)));
  }

  @Override
  public void deleteJob(JobDefinition definition) {
    execute("delete job " + definition.jobId(), () -> scheduler().deleteJob(jobKey(definition)));
  }

  @Override
  public void createTrigger(TriggerDefinition definition) {
    execute(
        "create trigger " + definition.triggerId(),
        () -> scheduler().scheduleJob(toTrigger(definition)));
  }

  @Override
  public void replaceTrigger(TriggerDefinition definition) {
    execute(
        "replace trigger " + definition.triggerId(),
        () -> {
          if (scheduler()
                  .rescheduleJob(
                      QuartzKeys.trigger(definition.triggerId(), definition.namespace()),
                      toTrigger(definition))
              == null) {
            throw new SchedulerException("Quartz trigger does not exist");
          }
        });
  }

  @Override
  public void pauseTrigger(TriggerDefinition definition) {
    execute(
        "pause trigger " + definition.triggerId(),
        () ->
            scheduler()
                .pauseTrigger(QuartzKeys.trigger(definition.triggerId(), definition.namespace())));
  }

  @Override
  public void resumeTrigger(TriggerDefinition definition) {
    execute(
        "resume trigger " + definition.triggerId(),
        () ->
            scheduler()
                .resumeTrigger(QuartzKeys.trigger(definition.triggerId(), definition.namespace())));
  }

  @Override
  public void deleteTrigger(TriggerDefinition definition) {
    execute(
        "delete trigger " + definition.triggerId(),
        () ->
            scheduler()
                .unscheduleJob(QuartzKeys.trigger(definition.triggerId(), definition.namespace())));
  }

  @Override
  public void fireTriggerNow(TriggerDefinition definition, UUID manualFireId) {
    JobDataMap data =
        new JobDataMap(
            Map.of(
                QuartzKeys.TRIGGER_ID, definition.triggerId().toString(),
                QuartzKeys.MANUAL_FIRE_ID, manualFireId.toString()));
    execute(
        "fire trigger now " + definition.triggerId(),
        () ->
            scheduler()
                .triggerJob(QuartzKeys.job(definition.jobId(), definition.namespace()), data));
  }

  private JobDetail toJobDetail(JobDefinition definition) {
    JobDataMap data =
        new JobDataMap(
            Map.of(
                QuartzKeys.JOB_ID, definition.jobId().toString(),
                QuartzKeys.NAMESPACE, definition.namespace(),
                QuartzKeys.REVISION, Long.toString(definition.revision())));

    Class<? extends org.quartz.Job> jobClass =
        definition.concurrencyPolicy() == ConcurrencyPolicy.DISALLOW
            ? NonConcurrentDispatchQuartzJob.class
            : ConcurrentDispatchQuartzJob.class;

    JobBuilder builder =
        JobBuilder.newJob(jobClass)
            .withIdentity(jobKey(definition))
            .withDescription(definition.description())
            .usingJobData(data)
            .storeDurably(definition.durable());
    if (definition.recoveryPolicy() == RecoveryPolicy.REQUEST_RECOVERY) {
      builder = builder.requestRecovery();
    }
    return builder.build();
  }

  private Trigger toTrigger(TriggerDefinition definition) {
    return triggerMapper.toQuartz(
        definition, QuartzKeys.job(definition.jobId(), definition.namespace()));
  }

  private Scheduler scheduler() {
    return Objects.requireNonNull(schedulerSupplier.get(), "schedulerSupplier returned null");
  }

  private static JobKey jobKey(JobDefinition definition) {
    return QuartzKeys.job(definition.jobId(), definition.namespace());
  }

  private static void execute(String operation, SchedulerOperation action) {
    try {
      action.run();
    } catch (SchedulerException exception) {
      throw new IllegalStateException("Failed to " + operation, exception);
    }
  }

  @FunctionalInterface
  private interface SchedulerOperation {
    void run() throws SchedulerException;
  }
}
