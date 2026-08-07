package io.k2iot.mcs.scheduler.observability;

import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.command.SchedulerProjectionPort;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.quartz.QuartzKeys;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.transaction.annotation.Transactional;

public final class QuartzReconciler {

  private final Scheduler scheduler;
  private final JobRepository jobRepository;
  private final TriggerRepository triggerRepository;
  private final SchedulerProjectionPort projection;
  private final SchedulerMetrics metrics;

  public QuartzReconciler(
      Scheduler scheduler,
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      SchedulerProjectionPort projection) {
    this(scheduler, jobRepository, triggerRepository, projection, null);
  }

  public QuartzReconciler(
      Scheduler scheduler,
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      SchedulerProjectionPort projection,
      SchedulerMetrics metrics) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
    this.triggerRepository = Objects.requireNonNull(triggerRepository, "triggerRepository");
    this.projection = Objects.requireNonNull(projection, "projection");
    this.metrics = metrics;
  }

  public ReconciliationReport reconcileJob(UUID jobId) {
    JobDefinition job = requireJob(jobId);
    List<Finding> findings = new ArrayList<>();
    try {
      if (job.state() != JobDefinition.State.DELETED
          && !scheduler.checkExists(QuartzKeys.job(job.jobId(), job.namespace()))) {
        findings.add(
            new Finding(
                FindingType.MISSING_QUARTZ_JOB,
                job.jobId(),
                job.namespace(),
                "Domain job has no Quartz JobDetail"));
      }
      for (TriggerDefinition trigger : triggerRepository.findByJobId(jobId)) {
        if (trigger.state() == TriggerDefinition.State.DELETED) {
          continue;
        }
        if (!scheduler.checkExists(QuartzKeys.trigger(trigger.triggerId(), trigger.namespace()))) {
          findings.add(
              new Finding(
                  FindingType.MISSING_QUARTZ_TRIGGER,
                  trigger.triggerId(),
                  trigger.namespace(),
                  "Domain trigger has no Quartz trigger"));
        }
      }
    } catch (SchedulerException exception) {
      throw new IllegalStateException("Cannot read Quartz metadata for reconciliation", exception);
    }
    findings.forEach(this::recordFinding);
    return new ReconciliationReport(findings);
  }

  @Transactional
  public void repairJob(UUID jobId, long expectedRevision) {
    JobDefinition job = requireJob(jobId);
    if (job.revision() != expectedRevision) {
      throw new SchedulerCommandException(
          "REVISION_CONFLICT",
          "Expected revision " + expectedRevision + " but found " + job.revision());
    }
    if (job.state() == JobDefinition.State.DELETED) {
      throw new SchedulerCommandException("RESOURCE_DELETED", "The job has already been deleted");
    }

    projection.updateJob(job);
    for (TriggerDefinition trigger : triggerRepository.findByJobId(jobId)) {
      if (trigger.state() != TriggerDefinition.State.DELETED) {
        projection.replaceTrigger(trigger);
      }
    }
    if (metrics != null) {
      metrics.recordRepair();
    }
  }

  private JobDefinition requireJob(UUID jobId) {
    return jobRepository
        .findById(Objects.requireNonNull(jobId, "jobId"))
        .orElseThrow(() -> new SchedulerCommandException("JOB_NOT_FOUND", "Job was not found"));
  }

  private void recordFinding(Finding finding) {
    if (metrics != null) {
      metrics.recordFinding(finding.type());
    }
  }

  public enum FindingType {
    MISSING_QUARTZ_JOB,
    MISSING_QUARTZ_TRIGGER
  }

  public record Finding(FindingType type, UUID aggregateId, String namespace, String detail) {}

  public record ReconciliationReport(List<Finding> findings) {
    public ReconciliationReport {
      findings = List.copyOf(findings);
    }
  }
}
