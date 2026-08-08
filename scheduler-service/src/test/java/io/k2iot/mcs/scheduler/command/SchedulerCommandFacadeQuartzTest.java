package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.k2iot.mcs.scheduler.SchedulerApplication;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.quartz.QuartzKeys;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest(
    classes = SchedulerApplication.class,
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
      "spring.flyway.enabled=true",
      "spring.grpc.server.port=0"
    })
class SchedulerCommandFacadeQuartzTest {

  private static final Instant NOW = Instant.parse("2026-08-07T03:00:00Z");
  private static final Instant START_AT = Instant.parse("2030-01-01T00:00:00Z");
  private static final UUID JOB_REQUEST_ID =
      UUID.fromString("10000000-0000-4000-8000-000000000001");
  private static final UUID TRIGGER_REQUEST_ID =
      UUID.fromString("10000000-0000-4000-8000-000000000002");
  private static final UUID SCHEDULE_REQUEST_ID =
      UUID.fromString("10000000-0000-4000-8000-000000000003");
  private static final UUID JOB_ID = UUID.fromString("20000000-0000-4000-8000-000000000001");
  private static final UUID CRON_TRIGGER_ID =
      UUID.fromString("30000000-0000-4000-8000-000000000001");
  private static final UUID ONCE_TRIGGER_ID =
      UUID.fromString("30000000-0000-4000-8000-000000000002");
  private static final UUID DESTINATION_ID =
      UUID.fromString("40000000-0000-4000-8000-000000000001");

  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("scheduler")
          .withUsername("scheduler")
          .withPassword("scheduler");

  static {
    POSTGRES.start();
  }

  @DynamicPropertySource
  static void registerPostgresProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
  }

  @Autowired JdbcTemplate jdbc;
  @Autowired SchedulerCommandFacade facade;
  @Autowired Scheduler scheduler;

  @BeforeEach
  void cleanState() throws Exception {
    scheduler.clear();
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
    jdbc.update("delete from scheduler.command_request");
    insertDestination();
  }

  @Test
  void rollsBackTriggerDomainRowWhenQuartzProjectionFails() throws Exception {
    facade.createJob(createJobCommand());

    assertThatThrownBy(() -> facade.createTrigger(createTriggerWithMissingCalendar()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to create trigger");

    assertThat(countById("scheduler.trigger_definition", "trigger_id", CRON_TRIGGER_ID)).isZero();
    assertThat(countById("scheduler.command_request", "request_id", TRIGGER_REQUEST_ID.toString()))
        .isZero();
    assertThat(scheduler.getTrigger(QuartzKeys.trigger(CRON_TRIGGER_ID, "billing"))).isNull();
    assertThat(countById("scheduler.job_definition", "job_id", JOB_ID)).isEqualTo(1);
  }

  @Test
  void createSchedulePersistsTwoDomainTriggersAndProjectsBothToQuartz() throws Exception {
    SchedulerCommands.ScheduleResult result = facade.createSchedule(createScheduleCommand());

    assertThat(result.triggers()).hasSize(2);
    assertThat(
            jdbc.queryForObject(
                "select count(*) from scheduler.trigger_definition where job_id = ?",
                Integer.class,
                JOB_ID))
        .isEqualTo(2);
    assertThat(scheduler.getTriggersOfJob(QuartzKeys.job(JOB_ID, "billing")))
        .extracting(trigger -> trigger.getKey())
        .containsExactlyInAnyOrder(
            QuartzKeys.trigger(CRON_TRIGGER_ID, "billing"),
            QuartzKeys.trigger(ONCE_TRIGGER_ID, "billing"));
  }

  private SchedulerCommands.CreateJob createJobCommand() {
    return new SchedulerCommands.CreateJob(JOB_REQUEST_ID, jobDraft(), "task-6-test");
  }

  private SchedulerCommands.CreateTrigger createTriggerWithMissingCalendar() {
    SchedulerCommands.TriggerDraft trigger =
        new SchedulerCommands.TriggerDraft(
            CRON_TRIGGER_ID,
            JOB_ID,
            "billing",
            "missing-calendar-trigger",
            "Trigger should fail Quartz projection",
            new CronTriggerSpec("0 0 8 * * ?"),
            START_AT,
            null,
            5,
            "UTC",
            TriggerDefinition.MisfirePolicy.DO_NOTHING,
            Set.of("missing-calendar"));
    return new SchedulerCommands.CreateTrigger(TRIGGER_REQUEST_ID, trigger, "task-6-test");
  }

  private SchedulerCommands.CreateSchedule createScheduleCommand() {
    SchedulerCommands.TriggerDraft cron =
        new SchedulerCommands.TriggerDraft(
            CRON_TRIGGER_ID,
            JOB_ID,
            "billing",
            "invoice-cron",
            "Daily invoice trigger",
            new CronTriggerSpec("0 0 8 * * ?"),
            START_AT,
            null,
            5,
            "UTC",
            TriggerDefinition.MisfirePolicy.DO_NOTHING,
            Set.of());
    SchedulerCommands.TriggerDraft once =
        new SchedulerCommands.TriggerDraft(
            ONCE_TRIGGER_ID,
            JOB_ID,
            "billing",
            "invoice-once",
            "One-shot invoice trigger",
            new OnceTriggerSpec(START_AT.plusSeconds(300)),
            START_AT,
            null,
            5,
            "UTC",
            TriggerDefinition.MisfirePolicy.FIRE_NOW,
            Set.of());
    return new SchedulerCommands.CreateSchedule(
        SCHEDULE_REQUEST_ID, jobDraft(), List.of(cron, once), "task-6-test");
  }

  private SchedulerCommands.JobDraft jobDraft() {
    return new SchedulerCommands.JobDraft(
        JOB_ID,
        "billing",
        "invoice-dispatch",
        "Dispatch invoice events",
        DESTINATION_ID,
        1,
        "billing.invoice.due",
        Map.of("invoiceId", "INV-2030-001"),
        Map.of("tenant", "mcs"),
        ConcurrencyPolicy.DISALLOW,
        RecoveryPolicy.REQUEST_RECOVERY,
        true);
  }

  private int countById(String table, String column, Object id) {
    Integer count =
        jdbc.queryForObject(
            "select count(*) from " + table + " where " + column + " = ?", Integer.class, id);
    return count == null ? 0 : count;
  }

  private void insertDestination() {
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, 'billing', 'KAFKA', 'billing.invoice.commands', '${jobId}',
                '{}'::jsonb, true, ?, 'task-6-test', ?, 'task-6-test')
        """,
        DESTINATION_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
  }
}
