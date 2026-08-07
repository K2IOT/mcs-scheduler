package io.k2iot.mcs.scheduler.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.command.SchedulerProjectionPort;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.quartz.QuartzKeys;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.Scheduler;

@ExtendWith(MockitoExtension.class)
class QuartzReconcilerTest {

  private static final Instant NOW = Instant.parse("2026-08-07T14:00:00Z");
  private static final UUID JOB_ID = UUID.fromString("20ba9c0b-5a71-4cf8-8191-1b4ae337d101");
  private static final UUID TRIGGER_ID =
      UUID.fromString("30a8a8dc-43c5-433e-a6cc-12efc0f5d102");
  private static final UUID DESTINATION_ID =
      UUID.fromString("59ee8102-5302-49df-9cb7-88fd46388d03");

  @Mock Scheduler scheduler;
  @Mock JobRepository jobRepository;
  @Mock TriggerRepository triggerRepository;
  @Mock SchedulerProjectionPort projection;

  @Test
  void reportsMissingQuartzTriggerWithoutAutomaticMutation() throws Exception {
    JobDefinition job = job();
    TriggerDefinition trigger = trigger();
    when(jobRepository.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(triggerRepository.findByJobId(JOB_ID)).thenReturn(java.util.List.of(trigger));
    when(scheduler.checkExists(QuartzKeys.job(JOB_ID, "billing"))).thenReturn(true);
    when(scheduler.checkExists(QuartzKeys.trigger(TRIGGER_ID, "billing"))).thenReturn(false);

    QuartzReconciler reconciler =
        new QuartzReconciler(scheduler, jobRepository, triggerRepository, projection);

    QuartzReconciler.ReconciliationReport report = reconciler.reconcileJob(JOB_ID);

    assertThat(report.findings())
        .singleElement()
        .satisfies(
            finding -> {
              assertThat(finding.type())
                  .isEqualTo(QuartzReconciler.FindingType.MISSING_QUARTZ_TRIGGER);
              assertThat(finding.aggregateId()).isEqualTo(TRIGGER_ID);
            });
    verify(projection, never()).createJob(job);
    verify(projection, never()).createTrigger(trigger);
  }

  @Test
  void explicitRepairReprojectsOnlyDomainOwnedActiveObjects() throws Exception {
    JobDefinition job = job();
    TriggerDefinition trigger = trigger();
    when(jobRepository.findById(JOB_ID)).thenReturn(Optional.of(job));
    when(triggerRepository.findByJobId(JOB_ID)).thenReturn(java.util.List.of(trigger));

    QuartzReconciler reconciler =
        new QuartzReconciler(scheduler, jobRepository, triggerRepository, projection);

    reconciler.repairJob(JOB_ID, 7);

    verify(projection).updateJob(job);
    verify(projection).replaceTrigger(trigger);
  }

  private JobDefinition job() {
    return new JobDefinition(
        JOB_ID,
        "billing",
        "reconcile-job",
        "Task 12 reconciliation job",
        DESTINATION_ID,
        1,
        "billing.reconcile",
        Map.of(),
        Map.of(),
        ConcurrencyPolicy.ALLOW,
        RecoveryPolicy.NONE,
        true,
        JobDefinition.State.ACTIVE,
        7,
        NOW,
        "test",
        NOW,
        "test");
  }

  private TriggerDefinition trigger() {
    return new TriggerDefinition(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "reconcile-trigger",
        "Task 12 reconciliation trigger",
        new OnceTriggerSpec(NOW.plusSeconds(3600)),
        NOW.plusSeconds(60),
        null,
        5,
        "UTC",
        TriggerDefinition.MisfirePolicy.FIRE_NOW,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        3,
        NOW,
        "test",
        NOW,
        "test");
  }
}
