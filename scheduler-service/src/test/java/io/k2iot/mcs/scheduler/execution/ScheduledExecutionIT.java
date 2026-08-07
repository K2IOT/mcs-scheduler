package io.k2iot.mcs.scheduler.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.outbox.JdbcOutboxRepository;
import io.k2iot.mcs.scheduler.outbox.OutboxRepository;
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
import org.junit.jupiter.api.Test;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.json.JsonMapper;

@Import(ScheduledExecutionIT.FailingOutboxConfiguration.class)
class ScheduledExecutionIT extends PostgresIntegrationTestBase {

  private static final Instant FIRE_TIME = Instant.parse("2026-08-07T10:00:00Z");
  private static final UUID JOB_ID = UUID.fromString("a1a0dc18-33e8-45ca-a27e-21928df40883");
  private static final UUID TRIGGER_ID = UUID.fromString("b537344b-e838-4e20-8b87-86bd4cdbbce4");
  private static final UUID DESTINATION_ID =
      UUID.fromString("623bfe28-581b-465c-89c8-5f1ca243c5d6");

  @Autowired ScheduledExecutionService scheduledExecutionService;
  @Autowired JobRepository jobRepository;
  @Autowired TriggerRepository triggerRepository;
  @Autowired JdbcTemplate jdbc;

  @Test
  void outboxConstraintFailureRollsBackExecutionInsert() {
    insertDestination();
    jobRepository.insert(job());
    triggerRepository.insert(trigger());

    assertThatThrownBy(() -> scheduledExecutionService.record(context()))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThat(
            jdbc.queryForObject(
                "select count(*) from scheduler.execution where execution_id = ?",
                Integer.class,
                ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME)))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "select count(*) from scheduler.outbox_event where aggregate_id = ?",
                Integer.class,
                ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME)))
        .isZero();
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
        "atomic-outbox-job",
        "Atomic execution/outbox test",
        DESTINATION_ID,
        3,
        "billing.invoice.due",
        Map.of("invoiceId", "INV-ATOMIC-001"),
        Map.of(),
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
        "atomic-outbox-trigger",
        "Atomic execution/outbox test",
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

  private JobExecutionContext context() {
    JobExecutionContext context = org.mockito.Mockito.mock(JobExecutionContext.class);
    JobDataMap data = new JobDataMap();
    data.put(QuartzKeys.JOB_ID, JOB_ID.toString());
    data.put(QuartzKeys.TRIGGER_ID, TRIGGER_ID.toString());
    data.put(QuartzKeys.NAMESPACE, "billing");
    when(context.getMergedJobDataMap()).thenReturn(data);
    when(context.getScheduledFireTime()).thenReturn(Date.from(FIRE_TIME));
    when(context.getFireTime()).thenReturn(Date.from(FIRE_TIME.plusMillis(250)));
    when(context.isRecovering()).thenReturn(false);
    return context;
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class FailingOutboxConfiguration {

    @Bean
    @Primary
    OutboxRepository failingOutboxRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
      JdbcOutboxRepository delegate = new JdbcOutboxRepository(jdbcClient, jsonMapper);
      return event -> {
        delegate.insert(event);
        delegate.insert(event);
      };
    }
  }
}
