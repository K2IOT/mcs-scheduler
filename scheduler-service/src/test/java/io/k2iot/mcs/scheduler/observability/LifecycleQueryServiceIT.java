package io.k2iot.mcs.scheduler.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.execution.ExecutionQueryService;
import io.k2iot.mcs.scheduler.execution.ExecutionRepository;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobQueryService;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerQueryService;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class LifecycleQueryServiceIT extends PostgresIntegrationTestBase {

  private static final Instant NOW = Instant.parse("2026-08-07T15:00:00Z");
  private static final UUID DESTINATION_ID =
      UUID.fromString("813aec94-ebba-4c36-bb73-a2bd2a04b901");

  @Autowired JdbcTemplate jdbc;
  @Autowired JobRepository jobRepository;
  @Autowired TriggerRepository triggerRepository;
  @Autowired ExecutionRepository executionRepository;
  @Autowired JobQueryService jobQueryService;
  @Autowired TriggerQueryService triggerQueryService;
  @Autowired ExecutionQueryService executionQueryService;

  @BeforeEach
  void cleanDatabase() {
    jdbc.update("delete from scheduler.outbox_event");
    jdbc.update("delete from scheduler.execution");
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
    insertDestination();
  }

  @Test
  void jobsUseKeysetPaginationAndCapPageSizeAtTwoHundred() {
    for (int index = 1; index <= 205; index++) {
      jobRepository.insert(job(index));
    }

    JobQueryService.Page first = jobQueryService.list("billing", 500, null);
    JobQueryService.Page second = jobQueryService.list("billing", 500, first.nextPageToken());

    assertThat(first.items()).hasSize(200);
    assertThat(first.nextPageToken()).isNotBlank();
    assertThat(second.items()).hasSize(5);
    assertThat(second.nextPageToken()).isNull();
    assertThat(first.items()).extracting(JobDefinition::jobId)
        .doesNotContainAnyElementsOf(second.items().stream().map(JobDefinition::jobId).toList());
  }

  @Test
  void triggersAndExecutionsContinueFromStableKeysetTokens() {
    JobDefinition job = job(500);
    jobRepository.insert(job);
    TriggerDefinition firstTrigger = trigger(job.jobId(), 1);
    TriggerDefinition secondTrigger = trigger(job.jobId(), 2);
    TriggerDefinition thirdTrigger = trigger(job.jobId(), 3);
    triggerRepository.insert(firstTrigger);
    triggerRepository.insert(secondTrigger);
    triggerRepository.insert(thirdTrigger);

    TriggerQueryService.Page triggerPage =
        triggerQueryService.listByJob(job.jobId(), "billing", 2, null);
    TriggerQueryService.Page triggerTail =
        triggerQueryService.listByJob(job.jobId(), "billing", 2, triggerPage.nextPageToken());

    assertThat(triggerPage.items()).hasSize(2);
    assertThat(triggerTail.items()).extracting(TriggerDefinition::triggerId)
        .containsExactly(thirdTrigger.triggerId());

    executionRepository.insertIfAbsent(execution(job.jobId(), firstTrigger.triggerId(), 1));
    executionRepository.insertIfAbsent(execution(job.jobId(), secondTrigger.triggerId(), 2));
    executionRepository.insertIfAbsent(execution(job.jobId(), thirdTrigger.triggerId(), 3));

    ExecutionQueryService.Page executionPage = executionQueryService.list("billing", 2, null);
    ExecutionQueryService.Page executionTail =
        executionQueryService.list("billing", 2, executionPage.nextPageToken());

    assertThat(executionPage.items()).hasSize(2);
    assertThat(executionTail.items()).hasSize(1);
    assertThat(executionTail.nextPageToken()).isNull();
  }

  private JobDefinition job(int index) {
    UUID jobId = new UUID(0L, index);
    Instant timestamp = NOW.plusMillis(index);
    return new JobDefinition(
        jobId,
        "billing",
        "job-" + index,
        "Task 12 query job",
        DESTINATION_ID,
        1,
        "billing.query",
        Map.of(),
        Map.of(),
        ConcurrencyPolicy.ALLOW,
        RecoveryPolicy.NONE,
        true,
        JobDefinition.State.ACTIVE,
        1,
        timestamp,
        "test",
        timestamp,
        "test");
  }

  private TriggerDefinition trigger(UUID jobId, int index) {
    UUID triggerId = new UUID(1L, index);
    Instant timestamp = NOW.plusSeconds(index);
    return new TriggerDefinition(
        triggerId,
        jobId,
        "billing",
        "trigger-" + index,
        "Task 12 query trigger",
        new OnceTriggerSpec(NOW.plusSeconds(3600 + index)),
        timestamp,
        null,
        5,
        "UTC",
        TriggerDefinition.MisfirePolicy.FIRE_NOW,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        timestamp,
        "test",
        timestamp,
        "test");
  }

  private ExecutionRepository.ExecutionRecord execution(UUID jobId, UUID triggerId, int index) {
    Instant fireTime = NOW.plusSeconds(100 + index);
    return new ExecutionRepository.ExecutionRecord(
        new UUID(2L, index),
        jobId,
        triggerId,
        null,
        fireTime,
        fireTime,
        ExecutionRepository.Status.SCHEDULED,
        1,
        Map.of("index", index),
        fireTime,
        fireTime);
  }

  private void insertDestination() {
    jdbc.update(
        """
        insert into scheduler.destination (
            destination_id, version, namespace, type, topic, key_expression, headers,
            enabled, created_at, created_by, updated_at, updated_by)
        values (?, 1, 'billing', 'KAFKA', 'billing.query', null,
                '{}'::jsonb, true, ?, 'test', ?, 'test')
        """,
        DESTINATION_ID,
        NOW.atOffset(ZoneOffset.UTC),
        NOW.atOffset(ZoneOffset.UTC));
  }
}
