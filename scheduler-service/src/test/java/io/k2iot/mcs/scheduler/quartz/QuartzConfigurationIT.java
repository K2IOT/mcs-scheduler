package io.k2iot.mcs.scheduler.quartz;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.SchedulerApplication;
import io.k2iot.mcs.scheduler.command.SchedulerProjectionPort;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.observability.QuartzReconciler;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerMetaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest(
    classes = SchedulerApplication.class,
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
      "spring.flyway.enabled=true"
    })
class QuartzConfigurationIT {

  private static final Instant CREATED_AT = Instant.parse("2026-08-07T00:00:00Z");
  private static final Instant START_AT = Instant.parse("2030-01-01T00:00:00Z");
  private static final UUID JOB_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");

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

  @Autowired private Scheduler scheduler;
  @Autowired private SchedulerProjectionPort projection;
  @Autowired(required = false)
  private QuartzReconciler reconciler;

  @AfterEach
  void clearQuartzState() throws Exception {
    scheduler.clear();
  }

  @Test
  void usesPersistentClusteredSpringManagedJobStore() throws Exception {
    SchedulerMetaData metadata = scheduler.getMetaData();

    assertThat(metadata.isJobStoreSupportsPersistence()).isTrue();
    assertThat(metadata.isJobStoreClustered()).isTrue();
    assertThat(metadata.getJobStoreClass().getName()).contains("LocalDataSourceJobStore");
    assertThat(scheduler.getSchedulerName()).isEqualTo("mcs-scheduler");
    assertThat(projection).isInstanceOf(QuartzSchedulerProjection.class);
    assertThat(reconciler).isNotNull();
  }

  @Test
  void oneQuartzJobCanOwnMultipleMappedTriggers() throws Exception {
    projection.createJob(job());
    projection.createTrigger(cronTrigger());
    projection.createTrigger(onceTrigger());

    assertThat(scheduler.getTriggersOfJob(QuartzKeys.job(JOB_ID, "billing")))
        .extracting(trigger -> trigger.getKey())
        .containsExactlyInAnyOrder(
            QuartzKeys.trigger(UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), "billing"),
            QuartzKeys.trigger(UUID.fromString("cccccccc-cccc-4ccc-8ccc-cccccccccccc"), "billing"));
  }

  private static JobDefinition job() {
    return new JobDefinition(
        JOB_ID,
        "billing",
        "renewal",
        "renewal job",
        UUID.fromString("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        1,
        "invoice.renewal",
        Map.of(),
        Map.of(),
        ConcurrencyPolicy.DISALLOW,
        RecoveryPolicy.REQUEST_RECOVERY,
        true,
        JobDefinition.State.ACTIVE,
        1,
        CREATED_AT,
        "test",
        CREATED_AT,
        "test");
  }

  private static TriggerDefinition cronTrigger() {
    return new TriggerDefinition(
        UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        JOB_ID,
        "billing",
        "renewal-cron",
        null,
        new CronTriggerSpec("0 0 8 * * ?"),
        START_AT,
        null,
        5,
        "Asia/Ho_Chi_Minh",
        TriggerDefinition.MisfirePolicy.DO_NOTHING,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        CREATED_AT,
        "test",
        CREATED_AT,
        "test");
  }

  private static TriggerDefinition onceTrigger() {
    Instant fireAt = START_AT.plusSeconds(300);
    return new TriggerDefinition(
        UUID.fromString("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        JOB_ID,
        "billing",
        "renewal-once",
        null,
        new OnceTriggerSpec(fireAt),
        START_AT,
        null,
        5,
        null,
        TriggerDefinition.MisfirePolicy.FIRE_NOW,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        CREATED_AT,
        "test",
        CREATED_AT,
        "test");
  }
}
