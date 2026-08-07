package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

@Import(PauseLifecycleIT.PauseLifecycleConfiguration.class)
class PauseLifecycleIT extends PostgresIntegrationTestBase {

  private static final Instant NOW = Instant.parse("2026-08-07T14:00:00Z");
  private static final UUID JOB_ID = UUID.fromString("15f6a80e-baa7-4bd3-98b6-8b26d7c1f201");
  private static final UUID MANUAL_TRIGGER_ID =
      UUID.fromString("5d281e30-f1d6-4761-bc73-868178417202");
  private static final UUID JOB_TRIGGER_ID =
      UUID.fromString("55fc1376-c164-4e35-9b85-cc124ec77303");
  private static final UUID DESTINATION_ID =
      UUID.fromString("38556799-1e2f-4b6b-9c05-528908e4e404");

  @Autowired JdbcTemplate jdbc;
  @Autowired SchedulerCommandFacade facade;

  @BeforeEach
  void cleanDatabase() {
    jdbc.update("delete from scheduler.audit_event");
    jdbc.update("delete from scheduler.command_request");
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
    insertDestination();
  }

  @Test
  void resumeJobOnlyResumesTriggersPausedByThatJob() {
    facade.createSchedule(
        new SchedulerCommands.CreateSchedule(
            request("0001"), jobDraft(), List.of(trigger(MANUAL_TRIGGER_ID), trigger(JOB_TRIGGER_ID)), "alice"));

    facade.pauseTrigger(
        new SchedulerCommands.TriggerMutation(
            request("0002"), MANUAL_TRIGGER_ID, "billing", 1, "alice"));
    facade.pauseJob(new SchedulerCommands.JobMutation(request("0003"), JOB_ID, "billing", 1, "alice"));
    facade.resumeJob(new SchedulerCommands.JobMutation(request("0004"), JOB_ID, "billing", 2, "alice"));

    assertThat(triggerState(MANUAL_TRIGGER_ID))
        .containsEntry("state", "PAUSED")
        .containsEntry("pause_reason", "INDIVIDUAL");
    assertThat(triggerState(JOB_TRIGGER_ID))
        .containsEntry("state", "ACTIVE")
        .containsEntry("pause_reason", null);
  }

  @Test
  void mutationsWriteSafeAuditMetadataInTheSameLifecycle() {
    facade.createSchedule(
        new SchedulerCommands.CreateSchedule(
            request("0011"), jobDraft(), List.of(trigger(MANUAL_TRIGGER_ID), trigger(JOB_TRIGGER_ID)), "alice"));
    facade.pauseTrigger(
        new SchedulerCommands.TriggerMutation(
            request("0012"), MANUAL_TRIGGER_ID, "billing", 1, "alice"));
    facade.pauseJob(new SchedulerCommands.JobMutation(request("0013"), JOB_ID, "billing", 1, "alice"));
    facade.resumeJob(new SchedulerCommands.JobMutation(request("0014"), JOB_ID, "billing", 2, "alice"));

    List<Map<String, Object>> audit =
        jdbc.queryForList(
            """
            select action, actor, correlation_id, payload::text as payload
            from scheduler.audit_event
            order by occurred_at, audit_event_id
            """);

    assertThat(audit).isNotEmpty();
    assertThat(audit).extracting(row -> row.get("actor")).containsOnly("alice");
    assertThat(audit).extracting(row -> row.get("action"))
        .contains("CREATE_JOB", "CREATE_TRIGGER", "PAUSE_TRIGGER", "PAUSE_JOB", "RESUME_JOB");
    assertThat(audit).allSatisfy(row -> assertThat(row.get("correlation_id")).isNotNull());
    assertThat(audit).allSatisfy(row -> assertThat((String) row.get("payload")).doesNotContain("invoiceId"));
  }

  private Map<String, Object> triggerState(UUID triggerId) {
    return jdbc.queryForMap(
        "select state, pause_reason from scheduler.trigger_definition where trigger_id = ?", triggerId);
  }

  private SchedulerCommands.JobDraft jobDraft() {
    return new SchedulerCommands.JobDraft(
        JOB_ID,
        "billing",
        "task-12-job",
        "Task 12 lifecycle job",
        DESTINATION_ID,
        1,
        "billing.task12",
        Map.of("invoiceId", "INV-SHOULD-NOT-BE-AUDITED"),
        Map.of("tenant", "mcs"),
        ConcurrencyPolicy.DISALLOW,
        RecoveryPolicy.NONE,
        true);
  }

  private SchedulerCommands.TriggerDraft trigger(UUID triggerId) {
    return new SchedulerCommands.TriggerDraft(
        triggerId,
        JOB_ID,
        "billing",
        "trigger-" + triggerId.toString().substring(0, 8),
        "Task 12 trigger",
        new OnceTriggerSpec(NOW.plusSeconds(7_200)),
        NOW.plusSeconds(60),
        null,
        5,
        "UTC",
        TriggerDefinition.MisfirePolicy.FIRE_NOW,
        Set.of());
  }

  private void insertDestination() {
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, 'billing', 'KAFKA', 'billing.task12', null,
                '{}'::jsonb, true, ?, 'test', ?, 'test')
        """,
        DESTINATION_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
  }

  private static UUID request(String suffix) {
    return UUID.fromString("00000000-0000-4000-8000-00000000" + suffix);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class PauseLifecycleConfiguration {

    @Bean
    Clock task12Clock() {
      return Clock.fixed(NOW, ZoneOffset.UTC);
    }

    @Bean
    SchedulerProjectionPort task12Projection() {
      return new NoOpProjection();
    }
  }

  static final class NoOpProjection implements SchedulerProjectionPort {
    @Override
    public void createJob(JobDefinition definition) {}

    @Override
    public void updateJob(JobDefinition definition) {}

    @Override
    public void pauseJob(JobDefinition definition) {}

    @Override
    public void resumeJob(JobDefinition definition) {}

    @Override
    public void deleteJob(JobDefinition definition) {}

    @Override
    public void createTrigger(TriggerDefinition definition) {}

    @Override
    public void replaceTrigger(TriggerDefinition definition) {}

    @Override
    public void pauseTrigger(TriggerDefinition definition) {}

    @Override
    public void resumeTrigger(TriggerDefinition definition) {}

    @Override
    public void deleteTrigger(TriggerDefinition definition) {}

    @Override
    public void fireTriggerNow(TriggerDefinition definition, UUID manualFireId) {}
  }
}
