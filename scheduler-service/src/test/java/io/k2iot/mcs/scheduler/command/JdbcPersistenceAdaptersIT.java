package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class JdbcPersistenceAdaptersIT extends PostgresIntegrationTestBase {

  private static final Instant NOW = Instant.parse("2026-08-06T10:00:00Z");
  private static final UUID DESTINATION_ID =
      UUID.fromString("6b5d06c8-6a48-4d88-9a20-46cbcb727bd9");
  private static final UUID JOB_ID = UUID.fromString("85fd9027-cf22-4e8c-af20-90a236c35e3c");
  private static final UUID TRIGGER_ID =
      UUID.fromString("985e8fe9-93bb-4b20-8686-b5660c67dc8f");

  @Autowired JdbcTemplate jdbc;
  @Autowired JobRepository jobRepository;
  @Autowired TriggerRepository triggerRepository;
  @Autowired CommandRequestRepository commandRequestRepository;
  @Autowired ObjectMapper objectMapper;

  @BeforeEach
  void cleanDatabase() {
    jdbc.update("delete from scheduler.trigger_definition");
    jdbc.update("delete from scheduler.job_definition");
    jdbc.update("delete from scheduler.destination");
    jdbc.update("delete from scheduler.command_request");
    insertDestination();
  }

  @Test
  void roundTripsJobAndTriggerDefinitionsIncludingJsonAndDurability() {
    JobDefinition job = job(1, JobDefinition.State.ACTIVE, NOW);
    jobRepository.insert(job);

    TriggerDefinition trigger =
        new TriggerDefinition(
            TRIGGER_ID,
            JOB_ID,
            "billing",
            "daily-invoice-due",
            "Every morning",
            new CronTriggerSpec("0 0 8 * * ?"),
            NOW.plusSeconds(60),
            null,
            5,
            "Asia/Ho_Chi_Minh",
            TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW,
            Set.of("business-days"),
            TriggerDefinition.State.ACTIVE,
            1,
            NOW,
            "integration-test",
            NOW,
            "integration-test");
    triggerRepository.insert(trigger);

    assertThat(jobRepository.findById(JOB_ID)).contains(job);
    assertThat(triggerRepository.findById(TRIGGER_ID)).contains(trigger);
    assertThat(triggerRepository.findByJobId(JOB_ID)).containsExactly(trigger);
  }

  @Test
  void updatesOnlyWhenExpectedRevisionMatches() {
    JobDefinition initial = job(1, JobDefinition.State.ACTIVE, NOW);
    jobRepository.insert(initial);
    JobDefinition updated = job(2, JobDefinition.State.PAUSED, NOW.plusSeconds(30));

    assertThat(jobRepository.update(updated, 1)).isTrue();
    assertThat(jobRepository.update(job(3, JobDefinition.State.ACTIVE, NOW.plusSeconds(60)), 1))
        .isFalse();
    assertThat(jobRepository.findById(JOB_ID)).contains(updated);
  }

  @Test
  void storesFingerprintAndCompletedResponseForIdempotentReplay() {
    UUID requestId = UUID.fromString("f0aacd2f-1e28-41f5-84ed-7f72bbc1ae11");
    CommandRequest request =
        CommandRequest.processing(
            UUID.randomUUID(),
            requestId,
            "CREATE_JOB",
            "billing",
            JOB_ID,
            "a".repeat(64),
            objectMapper.valueToTree(Map.of("jobId", JOB_ID.toString())),
            NOW);

    commandRequestRepository.insert(request);
    commandRequestRepository.complete(
        requestId, objectMapper.valueToTree(Map.of("jobId", JOB_ID.toString())), NOW.plusSeconds(1));

    CommandRequest stored = commandRequestRepository.findByRequestId(requestId).orElseThrow();
    assertThat(stored.requestHash()).isEqualTo("a".repeat(64));
    assertThat(stored.status()).isEqualTo(CommandRequest.Status.COMPLETED);
    assertThat(stored.responseJson().path("jobId").asText()).isEqualTo(JOB_ID.toString());
    assertThat(stored.processedAt()).isEqualTo(NOW.plusSeconds(1));
  }

  private JobDefinition job(long revision, JobDefinition.State state, Instant updatedAt) {
    return new JobDefinition(
        JOB_ID,
        "billing",
        "invoice-due",
        "Emit invoice due events",
        DESTINATION_ID,
        3,
        "billing.invoice.due",
        Map.of("invoiceId", "INV-2026-001", "attempt", 1),
        Map.of("tenant", "mcs"),
        ConcurrencyPolicy.DISALLOW,
        RecoveryPolicy.REQUEST_RECOVERY,
        true,
        state,
        revision,
        NOW,
        "integration-test",
        updatedAt,
        "integration-test");
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
        NOW,
        NOW);
  }
}
