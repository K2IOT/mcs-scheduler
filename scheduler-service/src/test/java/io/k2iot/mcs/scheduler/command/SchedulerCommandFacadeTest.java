package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class SchedulerCommandFacadeTest {

  private static final Instant NOW = Instant.parse("2026-08-06T09:30:00Z");
  private static final UUID REQUEST_ID = UUID.fromString("f0aacd2f-1e28-41f5-84ed-7f72bbc1ae11");
  private static final UUID JOB_ID = UUID.fromString("85fd9027-cf22-4e8c-af20-90a236c35e3c");
  private static final UUID DESTINATION_ID =
      UUID.fromString("6b5d06c8-6a48-4d88-9a20-46cbcb727bd9");

  @Mock JobRepository jobRepository;
  @Mock TriggerRepository triggerRepository;
  @Mock DestinationRepository destinationRepository;
  @Mock CommandRequestRepository commandRequestRepository;
  @Mock SchedulerProjectionPort schedulerProjection;

  private final JsonMapper jsonMapper = JsonMapper.builder().findAndAddModules().build();
  private SchedulerCommandFacade facade;

  @BeforeEach
  void setUp() {
    facade =
        new SchedulerCommandFacade(
            jobRepository,
            triggerRepository,
            destinationRepository,
            commandRequestRepository,
            schedulerProjection,
            jsonMapper,
            Clock.fixed(NOW, ZoneOffset.UTC));
  }

  @Test
  void createJobPersistsDomainThenProjectsAndCompletesRequest() {
    SchedulerCommands.CreateJob command = createJobCommand();
    when(commandRequestRepository.findByRequestId(REQUEST_ID)).thenReturn(Optional.empty());
    when(commandRequestRepository.insertIfAbsent(any(CommandRequest.class))).thenReturn(true);
    when(destinationRepository.findByIdAndVersion(DESTINATION_ID, 3))
        .thenReturn(Optional.of(destination(true, "billing")));

    JobDefinition result = facade.createJob(command);

    ArgumentCaptor<JobDefinition> definition = ArgumentCaptor.forClass(JobDefinition.class);
    verify(commandRequestRepository).insertIfAbsent(any(CommandRequest.class));
    verify(jobRepository).insert(definition.capture());
    verify(schedulerProjection).createJob(definition.getValue());
    verify(commandRequestRepository).complete(eq(REQUEST_ID), any(), eq(NOW));
    assertThat(result.jobId()).isEqualTo(JOB_ID);
    assertThat(result.revision()).isEqualTo(1);
    assertThat(result.createdAt()).isEqualTo(NOW);
    assertThat(result.durable()).isTrue();
  }

  @Test
  void rejectsDisabledDestinationBeforeWritingDomainState() {
    when(commandRequestRepository.findByRequestId(REQUEST_ID)).thenReturn(Optional.empty());
    when(commandRequestRepository.insertIfAbsent(any(CommandRequest.class))).thenReturn(true);
    when(destinationRepository.findByIdAndVersion(DESTINATION_ID, 3))
        .thenReturn(Optional.of(destination(false, "billing")));

    assertThatThrownBy(() -> facade.createJob(createJobCommand()))
        .isInstanceOf(SchedulerCommandException.class)
        .extracting(exception -> ((SchedulerCommandException) exception).code())
        .isEqualTo("DESTINATION_DISABLED");

    verify(jobRepository, never()).insert(any());
    verify(schedulerProjection, never()).createJob(any());
  }

  @Test
  void rejectsRequestIdReuseWithDifferentCanonicalPayload() {
    CommandRequest previous =
        CommandRequest.processing(
            UUID.randomUUID(),
            REQUEST_ID,
            "CREATE_JOB",
            "billing",
            JOB_ID,
            "0".repeat(64),
            jsonMapper.createObjectNode(),
            NOW.minusSeconds(10));
    when(commandRequestRepository.findByRequestId(REQUEST_ID)).thenReturn(Optional.of(previous));

    assertThatThrownBy(() -> facade.createJob(createJobCommand()))
        .isInstanceOf(SchedulerCommandException.class)
        .extracting(exception -> ((SchedulerCommandException) exception).code())
        .isEqualTo("IDEMPOTENCY_CONFLICT");

    verify(destinationRepository, never()).findByIdAndVersion(any(), any(Long.class));
    verify(jobRepository, never()).insert(any());
  }

  @Test
  void losingConcurrentClaimReplaysCompletedWinner() {
    SchedulerCommands.CreateJob command = createJobCommand();
    JobDefinition winner = expectedJob();
    String requestHash = RequestFingerprint.sha256(jsonMapper, command);
    CommandRequest completed =
        new CommandRequest(
            UUID.randomUUID(),
            REQUEST_ID,
            "CREATE_JOB",
            "billing",
            JOB_ID,
            requestHash,
            jsonMapper.valueToTree(command),
            CommandRequest.Status.COMPLETED,
            jsonMapper.valueToTree(winner),
            NOW.minusSeconds(1),
            NOW,
            null);
    when(commandRequestRepository.findByRequestId(REQUEST_ID))
        .thenReturn(Optional.empty(), Optional.of(completed));
    when(commandRequestRepository.insertIfAbsent(any(CommandRequest.class))).thenReturn(false);

    JobDefinition result = facade.createJob(command);

    assertThat(result).isEqualTo(winner);
    verify(destinationRepository, never()).findByIdAndVersion(any(), any(Long.class));
    verify(jobRepository, never()).insert(any());
    verify(schedulerProjection, never()).createJob(any());
    verify(commandRequestRepository, never()).complete(any(), any(), any());
  }

  private SchedulerCommands.CreateJob createJobCommand() {
    SchedulerCommands.JobDraft draft =
        new SchedulerCommands.JobDraft(
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
    return new SchedulerCommands.CreateJob(REQUEST_ID, draft, "scheduler-test");
  }

  private JobDefinition expectedJob() {
    return new JobDefinition(
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
        true,
        JobDefinition.State.ACTIVE,
        1,
        NOW,
        "scheduler-test",
        NOW,
        "scheduler-test");
  }

  private DestinationDefinition destination(boolean enabled, String namespace) {
    return new DestinationDefinition(
        DESTINATION_ID,
        3,
        namespace,
        DestinationDefinition.Type.KAFKA,
        "billing.invoice.commands",
        "${jobId}",
        Map.of(),
        enabled,
        NOW.minusSeconds(60),
        "admin",
        NOW.minusSeconds(60),
        "admin");
  }
}
