package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
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

@Import(SchedulerCommandFacadeIT.CommandTestConfiguration.class)
class SchedulerCommandFacadeIT extends PostgresIntegrationTestBase {

  private static final Instant NOW = Instant.parse("2026-08-06T10:30:00Z");
  private static final UUID REQUEST_ID = UUID.fromString("f0aacd2f-1e28-41f5-84ed-7f72bbc1ae11");
  private static final UUID TRIGGER_REQUEST_ID =
      UUID.fromString("c0195210-228d-430b-a4b4-f2351d761ec6");
  private static final UUID SCHEDULE_REQUEST_ID =
      UUID.fromString("484156b2-b477-447a-ae99-0f3e09532871");
  private static final UUID JOB_ID = UUID.fromString("85fd9027-cf22-4e8c-af20-90a236c35e3c");
  private static final UUID TRIGGER_ID = UUID.fromString("985e8fe9-93bb-4b20-8686-b5660c67dc8f");
  private static final UUID SECOND_TRIGGER_ID =
      UUID.fromString("d0b83d50-bb50-4a65-b17c-a84ac9b39cc3");
  private static final UUID DESTINATION_ID =
      UUID.fromString("6b5d06c8-6a48-4d88-9a20-46cbcb727bd9");

  @Autowired JdbcTemplate jdbc;
  @Autowired SchedulerCommandFacade facade;
  @Autowired ControllableProjection projection;

  @BeforeEach
  void cleanDatabase() {
    projection.reset();
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
    jdbc.update("delete from scheduler.command_request");
    insertDestination();
  }

  @Test
  void rollsBackCommandAndDomainRowsWhenProjectionFails() {
    projection.failJobCreation = true;

    assertThatThrownBy(() -> facade.createJob(createJobCommand()))
        .isInstanceOf(ProjectionFailure.class)
        .hasMessage("projection failed");

    assertThat(count("scheduler.job_definition")).isZero();
    assertThat(count("scheduler.command_request")).isZero();
  }

  @Test
  void completesTransactionAndReplaysWithoutProjectingAgain() {
    JobDefinition created = facade.createJob(createJobCommand());
    JobDefinition replayed = facade.createJob(createJobCommand());

    assertThat(replayed).isEqualTo(created);
    assertThat(count("scheduler.job_definition")).isEqualTo(1);
    assertThat(count("scheduler.command_request")).isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "select status from scheduler.command_request where request_id = ?",
                String.class,
                REQUEST_ID.toString()))
        .isEqualTo("COMPLETED");
    assertThat(projection.createdJobs).isEqualTo(1);
  }

  @Test
  void completedRequestReplaysStoredResponseAfterDomainRowChanges() {
    JobDefinition created = facade.createJob(createJobCommand());
    jdbc.update(
        "update scheduler.job_definition set name = 'changed-after-command', revision = 2 where job_id = ?",
        JOB_ID);

    JobDefinition replayed = facade.createJob(createJobCommand());

    assertThat(replayed).isEqualTo(created);
    assertThat(replayed.name()).isEqualTo("invoice-due");
    assertThat(replayed.revision()).isEqualTo(1);
    assertThat(projection.createdJobs).isEqualTo(1);
  }

  @Test
  void completedTriggerRequestReplaysStoredPolymorphicResponse() {
    facade.createJob(createJobCommand());
    TriggerDefinition created = facade.createTrigger(createTriggerCommand());
    jdbc.update(
        "update scheduler.trigger_definition set name = 'changed-trigger', revision = 2 where trigger_id = ?",
        TRIGGER_ID);

    TriggerDefinition replayed = facade.createTrigger(createTriggerCommand());

    assertThat(replayed).isEqualTo(created);
    assertThat(replayed.spec()).isEqualTo(new CronTriggerSpec("0 0 12 * * ?"));
    assertThat(replayed.revision()).isEqualTo(1);
    assertThat(projection.createdTriggers).isEqualTo(1);
  }

  @Test
  void createSchedulePersistsAndReplaysMultipleTriggersForOneJob() {
    SchedulerCommands.CreateSchedule command = createScheduleCommand();

    SchedulerCommands.ScheduleResult created = facade.createSchedule(command);
    SchedulerCommands.ScheduleResult replayed = facade.createSchedule(command);

    assertThat(created.triggers()).hasSize(2);
    assertThat(created.triggers()).extracting(TriggerDefinition::jobId).containsOnly(JOB_ID);
    assertThat(replayed).isEqualTo(created);
    assertThat(
            jdbc.queryForObject(
                "select count(*) from scheduler.trigger_definition where job_id = ?",
                Integer.class,
                JOB_ID))
        .isEqualTo(2);
    assertThat(projection.createdJobs).isEqualTo(1);
    assertThat(projection.createdTriggers).isEqualTo(2);
  }

  private SchedulerCommands.CreateJob createJobCommand() {
    return new SchedulerCommands.CreateJob(REQUEST_ID, jobDraft(), "integration-test");
  }

  private SchedulerCommands.JobDraft jobDraft() {
    return new SchedulerCommands.JobDraft(
        JOB_ID,
        "billing",
        "invoice-due",
        "Emit invoice due events",
        DESTINATION_ID,
        3,
        "billing.invoice.due",
        Map.of("invoiceId", "INV-2026-001"),
        Map.of("tenant", "mcs"),
        ConcurrencyPolicy.DISALLOW,
        RecoveryPolicy.REQUEST_RECOVERY,
        true);
  }

  private SchedulerCommands.CreateTrigger createTriggerCommand() {
    return new SchedulerCommands.CreateTrigger(
        TRIGGER_REQUEST_ID, cronTriggerDraft(), "integration-test");
  }

  private SchedulerCommands.CreateSchedule createScheduleCommand() {
    SchedulerCommands.TriggerDraft once =
        new SchedulerCommands.TriggerDraft(
            SECOND_TRIGGER_ID,
            JOB_ID,
            "billing",
            "invoice-once",
            "One-shot invoice scheduler",
            new OnceTriggerSpec(NOW.plusSeconds(7_200)),
            NOW.plusSeconds(60),
            null,
            5,
            "UTC",
            TriggerDefinition.MisfirePolicy.FIRE_NOW,
            Set.of());
    return new SchedulerCommands.CreateSchedule(
        SCHEDULE_REQUEST_ID, jobDraft(), List.of(cronTriggerDraft(), once), "integration-test");
  }

  private SchedulerCommands.TriggerDraft cronTriggerDraft() {
    return new SchedulerCommands.TriggerDraft(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "invoice-noon",
        "Run invoice scheduler at noon",
        new CronTriggerSpec("0 0 12 * * ?"),
        NOW.plusSeconds(60),
        null,
        5,
        "UTC",
        TriggerDefinition.MisfirePolicy.DO_NOTHING,
        Set.of());
  }

  private int count(String table) {
    Integer count = jdbc.queryForObject("select count(*) from " + table, Integer.class);
    return count == null ? 0 : count;
  }

  private void insertDestination() {
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 3, 'billing', 'KAFKA', 'billing.invoice.commands', '${jobId}',
                '{}'::jsonb, true, ?, 'integration-test', ?, 'integration-test')
        """,
        DESTINATION_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class CommandTestConfiguration {

    @Bean
    Clock commandClock() {
      return Clock.fixed(NOW, ZoneOffset.UTC);
    }

    @Bean
    ControllableProjection schedulerProjectionPort() {
      return new ControllableProjection();
    }
  }

  static final class ControllableProjection implements SchedulerProjectionPort {

    private boolean failJobCreation;
    private int createdJobs;
    private int createdTriggers;

    void reset() {
      failJobCreation = false;
      createdJobs = 0;
      createdTriggers = 0;
    }

    @Override
    public void createJob(JobDefinition definition) {
      if (failJobCreation) {
        throw new ProjectionFailure("projection failed");
      }
      createdJobs++;
    }

    @Override
    public void updateJob(JobDefinition definition) {}

    @Override
    public void pauseJob(JobDefinition definition) {}

    @Override
    public void resumeJob(JobDefinition definition) {}

    @Override
    public void deleteJob(JobDefinition definition) {}

    @Override
    public void createTrigger(TriggerDefinition definition) {
      createdTriggers++;
    }

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

  static final class ProjectionFailure extends RuntimeException {

    ProjectionFailure(String message) {
      super(message);
    }
  }
}
