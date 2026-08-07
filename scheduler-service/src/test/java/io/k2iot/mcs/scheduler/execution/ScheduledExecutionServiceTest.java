package io.k2iot.mcs.scheduler.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.destination.DestinationDefinition;
import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.outbox.OutboxRepository;
import io.k2iot.mcs.scheduler.quartz.QuartzKeys;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class ScheduledExecutionServiceTest {

  private static final Instant FIRE_TIME = Instant.parse("2026-08-07T09:00:00Z");
  private static final Instant ACTUAL_FIRE_TIME = Instant.parse("2026-08-07T09:00:01Z");
  private static final UUID JOB_ID = UUID.fromString("e20d44df-0fd1-48b6-bd9b-1a8b8d7c4201");
  private static final UUID TRIGGER_ID = UUID.fromString("3ba6e7bd-58b7-4471-9333-841675527f33");
  private static final UUID DESTINATION_ID =
      UUID.fromString("af18fb4d-4b98-4cef-8f7f-aee92bd3e07d");

  @Mock JobRepository jobRepository;
  @Mock TriggerRepository triggerRepository;
  @Mock DestinationRepository destinationRepository;
  @Mock ExecutionRepository executionRepository;
  @Mock OutboxRepository outboxRepository;

  private ScheduledExecutionService service;

  @BeforeEach
  void setUp() {
    JsonMapper jsonMapper = JsonMapper.builder().findAndAddModules().build();
    service =
        new ScheduledExecutionService(
            jobRepository,
            triggerRepository,
            destinationRepository,
            executionRepository,
            outboxRepository,
            new ExecutionEventFactory(jsonMapper),
            Clock.fixed(ACTUAL_FIRE_TIME, ZoneOffset.UTC));

    when(jobRepository.findById(JOB_ID)).thenReturn(Optional.of(job(JobDefinition.State.ACTIVE)));
    when(triggerRepository.findById(TRIGGER_ID))
        .thenReturn(Optional.of(trigger(TriggerDefinition.State.ACTIVE)));
    when(destinationRepository.findByIdAndVersion(DESTINATION_ID, 3))
        .thenReturn(Optional.of(destination(true)));
  }

  @Test
  void repeatedRecoveryCreatesOneLogicalExecution() {
    when(executionRepository.insertIfAbsent(any())).thenReturn(true, false);

    service.record(contextFor(false, null));
    service.record(contextFor(true, null));

    verify(executionRepository, times(2)).insertIfAbsent(any());
    verify(outboxRepository, times(1)).insert(any());
  }

  @Test
  void pausedDefinitionCreatesSuppressedExecutionWithoutOutbox() {
    when(triggerRepository.findById(TRIGGER_ID))
        .thenReturn(Optional.of(trigger(TriggerDefinition.State.PAUSED)));
    when(executionRepository.insertIfAbsent(any())).thenReturn(true);

    service.record(contextFor(false, null));

    ArgumentCaptor<ExecutionRepository.ExecutionRecord> execution =
        ArgumentCaptor.forClass(ExecutionRepository.ExecutionRecord.class);
    verify(executionRepository).insertIfAbsent(execution.capture());
    verify(outboxRepository, never()).insert(any());
    assertThat(execution.getValue().status()).isEqualTo(ExecutionRepository.Status.SUPPRESSED);
  }

  @Test
  void manualFireUsesManualFireIdAsExecutionIdentity() {
    UUID manualFireId = UUID.fromString("e2139734-b157-4024-b0a0-fb00884f17e8");
    when(executionRepository.insertIfAbsent(any())).thenReturn(true);

    service.record(contextFor(false, manualFireId));

    ArgumentCaptor<ExecutionRepository.ExecutionRecord> execution =
        ArgumentCaptor.forClass(ExecutionRepository.ExecutionRecord.class);
    verify(executionRepository).insertIfAbsent(execution.capture());
    assertThat(execution.getValue().executionId()).isEqualTo(manualFireId);
    assertThat(execution.getValue().manualFireId()).isEqualTo(manualFireId);
    assertThat(execution.getValue().triggerId()).isNull();
    assertThat(execution.getValue().scheduledFireTime()).isNull();
  }

  private JobExecutionContext contextFor(boolean recovering, UUID manualFireId) {
    JobExecutionContext context = org.mockito.Mockito.mock(JobExecutionContext.class);
    JobDataMap data = new JobDataMap();
    data.put(QuartzKeys.JOB_ID, JOB_ID.toString());
    data.put(QuartzKeys.TRIGGER_ID, TRIGGER_ID.toString());
    data.put(QuartzKeys.NAMESPACE, "billing");
    if (manualFireId != null) {
      data.put(QuartzKeys.MANUAL_FIRE_ID, manualFireId.toString());
    }
    when(context.getMergedJobDataMap()).thenReturn(data);
    when(context.getScheduledFireTime()).thenReturn(Date.from(FIRE_TIME));
    when(context.getFireTime()).thenReturn(Date.from(ACTUAL_FIRE_TIME));
    when(context.isRecovering()).thenReturn(recovering);
    return context;
  }

  private JobDefinition job(JobDefinition.State state) {
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
        ConcurrencyPolicy.ALLOW,
        RecoveryPolicy.REQUEST_RECOVERY,
        true,
        state,
        4,
        FIRE_TIME.minusSeconds(3600),
        "scheduler-test",
        FIRE_TIME.minusSeconds(60),
        "scheduler-test");
  }

  private TriggerDefinition trigger(TriggerDefinition.State state) {
    return new TriggerDefinition(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "invoice-due-once",
        "One invoice event",
        new OnceTriggerSpec(FIRE_TIME),
        FIRE_TIME,
        null,
        5,
        "UTC",
        TriggerDefinition.MisfirePolicy.FIRE_NOW,
        Set.of(),
        state,
        2,
        FIRE_TIME.minusSeconds(3600),
        "scheduler-test",
        FIRE_TIME.minusSeconds(60),
        "scheduler-test");
  }

  private DestinationDefinition destination(boolean enabled) {
    return new DestinationDefinition(
        DESTINATION_ID,
        3,
        "billing",
        DestinationDefinition.Type.KAFKA,
        "billing.events.v1",
        null,
        Map.of("content-type", "application/json"),
        enabled,
        FIRE_TIME.minusSeconds(7200),
        "scheduler-test",
        FIRE_TIME.minusSeconds(7200),
        "scheduler-test");
  }
}
