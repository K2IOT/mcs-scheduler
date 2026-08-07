package io.k2iot.mcs.scheduler.quartz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;

@ExtendWith(MockitoExtension.class)
class QuartzSchedulerProjectionTest {

  private static final Instant NOW = Instant.parse("2026-08-07T01:00:00Z");
  private static final UUID JOB_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
  private static final UUID TRIGGER_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");

  @Mock private Scheduler scheduler;

  private QuartzSchedulerProjection projection;

  @BeforeEach
  void setUp() {
    projection = new QuartzSchedulerProjection(scheduler, new QuartzTriggerMapper());
  }

  @Test
  void createsConcurrentJobWithStringOnlyQuartzDataAndRecoverySettings() throws Exception {
    JobDefinition definition =
        job(ConcurrencyPolicy.ALLOW, RecoveryPolicy.REQUEST_RECOVERY, false, 4L);

    projection.createJob(definition);

    ArgumentCaptor<JobDetail> detailCaptor = ArgumentCaptor.forClass(JobDetail.class);
    verify(scheduler).addJob(detailCaptor.capture(), eq(false), eq(true));
    JobDetail detail = detailCaptor.getValue();

    assertThat(detail.getKey()).isEqualTo(QuartzKeys.job(JOB_ID, "billing"));
    assertThat(detail.getJobClass()).isEqualTo(ConcurrentDispatchQuartzJob.class);
    assertThat(detail.isDurable()).isFalse();
    assertThat(detail.requestsRecovery()).isTrue();
    assertThat(detail.getJobDataMap())
        .containsEntry(QuartzKeys.JOB_ID, JOB_ID.toString())
        .containsEntry(QuartzKeys.NAMESPACE, "billing")
        .containsEntry(QuartzKeys.REVISION, "4");
    assertThat(detail.getJobDataMap().values()).allMatch(String.class::isInstance);
  }

  @Test
  void createsNonConcurrentQuartzJobForDisallowPolicy() throws Exception {
    projection.createJob(job(ConcurrencyPolicy.DISALLOW, RecoveryPolicy.NONE, true, 1L));

    ArgumentCaptor<JobDetail> detailCaptor = ArgumentCaptor.forClass(JobDetail.class);
    verify(scheduler).addJob(detailCaptor.capture(), eq(false), eq(true));
    assertThat(detailCaptor.getValue().getJobClass())
        .isEqualTo(NonConcurrentDispatchQuartzJob.class);
    assertThat(detailCaptor.getValue().isDurable()).isTrue();
    assertThat(detailCaptor.getValue().requestsRecovery()).isFalse();
  }

  @Test
  void schedulesMappedTriggerAgainstExistingJob() throws Exception {
    TriggerDefinition definition = trigger();

    projection.createTrigger(definition);

    ArgumentCaptor<Trigger> triggerCaptor = ArgumentCaptor.forClass(Trigger.class);
    verify(scheduler).scheduleJob(triggerCaptor.capture());
    assertThat(triggerCaptor.getValue().getKey())
        .isEqualTo(QuartzKeys.trigger(TRIGGER_ID, "billing"));
    assertThat(triggerCaptor.getValue().getJobKey()).isEqualTo(QuartzKeys.job(JOB_ID, "billing"));
  }

  @Test
  void manualFireCarriesStableStringIdentifiersInMergedJobData() throws Exception {
    UUID manualFireId = UUID.fromString("cccccccc-cccc-4ccc-8ccc-cccccccccccc");

    projection.fireTriggerNow(trigger(), manualFireId);

    ArgumentCaptor<JobDataMap> dataCaptor = ArgumentCaptor.forClass(JobDataMap.class);
    verify(scheduler).triggerJob(eq(QuartzKeys.job(JOB_ID, "billing")), dataCaptor.capture());
    assertThat(dataCaptor.getValue())
        .containsEntry(QuartzKeys.TRIGGER_ID, TRIGGER_ID.toString())
        .containsEntry(QuartzKeys.MANUAL_FIRE_ID, manualFireId.toString());
    assertThat(dataCaptor.getValue().values()).allMatch(String.class::isInstance);
  }

  private static JobDefinition job(
      ConcurrencyPolicy concurrencyPolicy,
      RecoveryPolicy recoveryPolicy,
      boolean durable,
      long revision) {
    return new JobDefinition(
        JOB_ID,
        "billing",
        "renewal",
        "renewal job",
        UUID.fromString("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        1,
        "invoice.renewal",
        Map.of("customerId", "42"),
        Map.of("source", "scheduler"),
        concurrencyPolicy,
        recoveryPolicy,
        durable,
        JobDefinition.State.ACTIVE,
        revision,
        NOW.minusSeconds(60),
        "test",
        NOW,
        "test");
  }

  private static TriggerDefinition trigger() {
    return new TriggerDefinition(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "renewal-cron",
        "renewal trigger",
        new CronTriggerSpec("0 0 8 * * ?"),
        NOW.plusSeconds(60),
        null,
        5,
        "Asia/Ho_Chi_Minh",
        TriggerDefinition.MisfirePolicy.DO_NOTHING,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        NOW.minusSeconds(60),
        "test",
        NOW,
        "test");
  }
}
