package io.k2iot.mcs.scheduler.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.quartz.QuartzKeys;
import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class ScheduledExecutionIdempotencyIT extends PostgresIntegrationTestBase {

  private static final Instant FIRE_TIME = Instant.parse("2026-08-07T11:00:00Z");
  private static final Instant ACTUAL_FIRE_TIME = Instant.parse("2026-08-07T11:00:01Z");
  private static final UUID JOB_ID = UUID.fromString("71000000-0000-4000-8000-000000000001");
  private static final UUID TRIGGER_ID =
      UUID.fromString("72000000-0000-4000-8000-000000000001");
  private static final UUID DESTINATION_ID =
      UUID.fromString("73000000-0000-4000-8000-000000000001");

  @Autowired ScheduledExecutionService scheduledExecutionService;
  @Autowired JobRepository jobRepository;
  @Autowired TriggerRepository triggerRepository;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void cleanAndSeedState() {
    jdbc.update("delete from scheduler.outbox_event");
    jdbc.update("delete from scheduler.execution");
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");

    insertDestination();
    jobRepository.insert(job());
    triggerRepository.insert(trigger());
  }

  @Test
  void repeatedScheduledFireCreatesExactlyOneExecutionAndOutbox() {
    scheduledExecutionService.record(scheduledContext());
    scheduledExecutionService.record(scheduledContext());

    assertExactlyOneExecutionAndOutbox(ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME));
  }

  @Test
  void repeatedRecoveryCreatesExactlyOneExecutionAndOutbox() {
    scheduledExecutionService.record(scheduledContext());
    scheduledExecutionService.record(recoveryContext());

    assertExactlyOneExecutionAndOutbox(ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME));
  }

  @Test
  void repeatedManualFireIdCreatesExactlyOneExecutionAndOutbox() {
    UUID manualFireId = UUID.fromString("74000000-0000-4000-8000-000000000001");

    scheduledExecutionService.record(manualContext(manualFireId));
    scheduledExecutionService.record(manualContext(manualFireId));

    assertExactlyOneExecutionAndOutbox(manualFireId);
    assertThat(
            jdbc.queryForObject(
                """
                select count(*)
                from scheduler.execution
                where execution_id = ?
                  and manual_fire_id = ?
                  and trigger_id is null
                  and scheduled_fire_time is null
                """,
                Integer.class,
                manualFireId,
                manualFireId))
        .isEqualTo(1);
  }

  private void assertExactlyOneExecutionAndOutbox(UUID executionId) {
    assertThat(
            jdbc.queryForObject(
                "select count(*) from scheduler.execution where execution_id = ?",
                Integer.class,
                executionId))
        .isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "select count(*) from scheduler.outbox_event where aggregate_id = ?",
                Integer.class,
                executionId))
        .isEqualTo(1);
  }

  private void insertDestination() {
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers, enabled,
            created_at, created_by, updated_at, updated_by)
        values (?, ?, ?, ?, ?, ?, cast(? as jsonb), ?, ?, ?, ?, ?)
        """,
        DESTINATION_ID,
        3L,
        "billing",
        DestinationDefinition.Type.KAFKA.name(),
        "billing.events.v1",
        null,
        "{}",
        true,
        OffsetDateTime.ofInstant(FIRE_TIME.minusSeconds(7200), ZoneOffset.UTC),
        "scheduler-test",
        OffsetDateTime.ofInstant(FIRE_TIME.minusSeconds(7200), ZoneOffset.UTC),
        "scheduler-test");
  }

  private JobDefinition job() {
    return new JobDefinition(
        JOB_ID,
        "billing",
        "idempotent-execution-job",
        "Durable execution idempotency test",
        DESTINATION_ID,
        3,
        "billing.invoice.due",
        Map.of("invoiceId", "INV-IDEMPOTENT-001"),
        Map.of("tenant", "mcs"),
        ConcurrencyPolicy.ALLOW,
        RecoveryPolicy.REQUEST_RECOVERY,
        true,
        JobDefinition.State.ACTIVE,
        1,
        FIRE_TIME.minusSeconds(3600),
        "scheduler-test",
        FIRE_TIME.minusSeconds(3600),
        "scheduler-test");
  }

  private TriggerDefinition trigger() {
    return new TriggerDefinition(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "idempotent-execution-trigger",
        "Durable execution idempotency test",
        new OnceTriggerSpec(FIRE_TIME),
        FIRE_TIME,
        null,
        5,
        "UTC",
        TriggerDefinition.MisfirePolicy.FIRE_NOW,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        FIRE_TIME.minusSeconds(3600),
        "scheduler-test",
        FIRE_TIME.minusSeconds(3600),
        "scheduler-test");
  }

  private JobExecutionContext scheduledContext() {
    JobExecutionContext context = org.mockito.Mockito.mock(JobExecutionContext.class);
    JobDataMap data = jobDataMap(true, null);
    when(context.getMergedJobDataMap()).thenReturn(data);
    when(context.getScheduledFireTime()).thenReturn(Date.from(FIRE_TIME));
    when(context.getFireTime()).thenReturn(Date.from(ACTUAL_FIRE_TIME));
    when(context.isRecovering()).thenReturn(false);
    return context;
  }

  private JobExecutionContext recoveryContext() {
    JobExecutionContext context = org.mockito.Mockito.mock(JobExecutionContext.class);
    JobDataMap data = jobDataMap(false, null);
    when(context.getMergedJobDataMap()).thenReturn(data);
    when(context.isRecovering()).thenReturn(true);
    when(context.getRecoveringTriggerKey())
        .thenReturn(new TriggerKey(TRIGGER_ID.toString(), "billing"));
    when(context.getScheduledFireTime()).thenReturn(Date.from(FIRE_TIME));
    when(context.getFireTime()).thenReturn(Date.from(ACTUAL_FIRE_TIME));
    return context;
  }

  private JobExecutionContext manualContext(UUID manualFireId) {
    JobExecutionContext context = org.mockito.Mockito.mock(JobExecutionContext.class);
    JobDataMap data = jobDataMap(true, manualFireId);
    when(context.getMergedJobDataMap()).thenReturn(data);
    when(context.getFireTime()).thenReturn(Date.from(ACTUAL_FIRE_TIME));
    when(context.isRecovering()).thenReturn(false);
    return context;
  }

  private JobDataMap jobDataMap(boolean includeTriggerId, UUID manualFireId) {
    JobDataMap data = new JobDataMap();
    data.put(QuartzKeys.JOB_ID, JOB_ID.toString());
    data.put(QuartzKeys.NAMESPACE, "billing");
    if (includeTriggerId) {
      data.put(QuartzKeys.TRIGGER_ID, TRIGGER_ID.toString());
    }
    if (manualFireId != null) {
      data.put(QuartzKeys.MANUAL_FIRE_ID, manualFireId.toString());
    }
    return data;
  }
}
