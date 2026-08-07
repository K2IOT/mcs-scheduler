package io.k2iot.mcs.scheduler.grpc;

import static io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy.CONCURRENCY_POLICY_DISALLOW;
import static io.k2iot.mcs.scheduler.v1.MisfirePolicy.MISFIRE_POLICY_DO_NOTHING;
import static io.k2iot.mcs.scheduler.v1.RecoveryPolicy.RECOVERY_POLICY_REQUEST_RECOVERY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.k2iot.mcs.scheduler.command.ManualFireResult;
import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.SimpleIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.v1.CreateJobRequest;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.CreateTriggerRequest;
import io.k2iot.mcs.scheduler.v1.CronTrigger;
import io.k2iot.mcs.scheduler.v1.DeleteJobRequest;
import io.k2iot.mcs.scheduler.v1.DeleteTriggerRequest;
import io.k2iot.mcs.scheduler.v1.FireTriggerRequest;
import io.k2iot.mcs.scheduler.v1.JobDraft;
import io.k2iot.mcs.scheduler.v1.JobMutationRequest;
import io.k2iot.mcs.scheduler.v1.ReplaceTriggerRequest;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.SchedulerCommandServiceGrpc;
import io.k2iot.mcs.scheduler.v1.SimpleIntervalTrigger;
import io.k2iot.mcs.scheduler.v1.TriggerDraft;
import io.k2iot.mcs.scheduler.v1.TriggerMutationRequest;
import io.k2iot.mcs.scheduler.v1.TriggerSpec;
import io.k2iot.mcs.scheduler.v1.UpdateJobRequest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureInProcessTransport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(SchedulerCommandGrpcServiceTest.TestConfig.class)
@AutoConfigureInProcessTransport
@TestPropertySource(
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,org.springframework.boot.quartz.autoconfigure.QuartzAutoConfiguration,org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration,org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
    })
class SchedulerCommandGrpcServiceTest {

  private static final Instant NOW = Instant.parse("2026-08-07T03:00:00Z");
  private static final Instant START_AT = Instant.parse("2030-01-01T00:00:00Z");
  private static final UUID REQUEST_ID = UUID.fromString("10000000-0000-4000-8000-000000000008");
  private static final UUID METADATA_REQUEST_ID =
      UUID.fromString("10000000-0000-4000-8000-000000000009");
  private static final UUID JOB_ID = UUID.fromString("20000000-0000-4000-8000-000000000008");
  private static final UUID TRIGGER_ID = UUID.fromString("30000000-0000-4000-8000-000000000008");
  private static final UUID DESTINATION_ID =
      UUID.fromString("40000000-0000-4000-8000-000000000008");
  private static final UUID MANUAL_FIRE_ID =
      UUID.fromString("50000000-0000-4000-8000-000000000008");

  @Autowired SchedulerCommandFacade facade;
  @Autowired GrpcChannelFactory channelFactory;

  private SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub stub;

  @BeforeEach
  void setUp() {
    reset(facade);
    stub =
        SchedulerCommandServiceGrpc.newBlockingStub(channelFactory.createChannel("localhost:9090"));
  }

  @Test
  void createsScheduleWithSameSemanticsAsRest() {
    when(facade.createSchedule(any())).thenReturn(scheduleResult());

    ScheduleResponse response = stub.createSchedule(validGrpcRequest());

    assertThat(response.getJob().getNamespace()).isEqualTo("billing");
    assertThat(response.getTriggersCount()).isEqualTo(1);

    ArgumentCaptor<SchedulerCommands.CreateSchedule> command =
        ArgumentCaptor.forClass(SchedulerCommands.CreateSchedule.class);
    verify(facade).createSchedule(command.capture());
    assertThat(command.getValue().requestId()).isEqualTo(REQUEST_ID);
    assertThat(command.getValue().actor()).isEqualTo("grpc-request");
    assertThat(command.getValue().job().namespace()).isEqualTo("billing");
    assertThat(command.getValue().triggers())
        .singleElement()
        .extracting(SchedulerCommands.TriggerDraft::timezone)
        .isEqualTo("UTC");
  }

  @Test
  void delegatesEveryCommandRpcToSharedFacade() {
    when(facade.createJob(any())).thenReturn(job());
    when(facade.updateJob(any())).thenReturn(job());
    when(facade.pauseJob(any())).thenReturn(job());
    when(facade.resumeJob(any())).thenReturn(job());
    when(facade.deleteJob(any())).thenReturn(job());
    when(facade.createTrigger(any())).thenReturn(trigger());
    when(facade.replaceTrigger(any())).thenReturn(trigger());
    when(facade.pauseTrigger(any())).thenReturn(trigger());
    when(facade.resumeTrigger(any())).thenReturn(trigger());
    when(facade.deleteTrigger(any())).thenReturn(trigger());
    when(facade.fireTriggerNow(any()))
        .thenReturn(new ManualFireResult(MANUAL_FIRE_ID, TRIGGER_ID, JOB_ID));
    when(facade.createSchedule(any())).thenReturn(scheduleResult());

    stub.createJob(
        CreateJobRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setJob(grpcJobDraft())
            .build());
    stub.updateJob(
        UpdateJobRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setExpectedRevision(1)
            .setJob(grpcJobDraft())
            .build());
    stub.pauseJob(jobMutationRequest());
    stub.resumeJob(jobMutationRequest());
    stub.deleteJob(
        DeleteJobRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setJobId(JOB_ID.toString())
            .setExpectedRevision(1)
            .build());
    stub.createTrigger(
        CreateTriggerRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setTrigger(grpcTriggerDraft())
            .build());
    stub.replaceTrigger(
        ReplaceTriggerRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setExpectedRevision(1)
            .setTrigger(grpcTriggerDraft())
            .build());
    stub.pauseTrigger(triggerMutationRequest());
    stub.resumeTrigger(triggerMutationRequest());
    stub.deleteTrigger(
        DeleteTriggerRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setTriggerId(TRIGGER_ID.toString())
            .setExpectedRevision(1)
            .build());
    stub.fireTriggerNow(
        FireTriggerRequest.newBuilder()
            .setRequestId(REQUEST_ID.toString())
            .setNamespace("billing")
            .setCaller("grpc-request")
            .setTriggerId(TRIGGER_ID.toString())
            .setManualFireId(MANUAL_FIRE_ID.toString())
            .build());
    stub.createSchedule(validGrpcRequest());

    verify(facade).createJob(any());
    verify(facade).updateJob(any());
    verify(facade).pauseJob(any());
    verify(facade).resumeJob(any());
    verify(facade).deleteJob(any());
    verify(facade).createTrigger(any());
    verify(facade).replaceTrigger(any());
    verify(facade).pauseTrigger(any());
    verify(facade).resumeTrigger(any());
    verify(facade).deleteTrigger(any());
    verify(facade).fireTriggerNow(any());
    verify(facade).createSchedule(any());
  }

  @Test
  void mapsRevisionConflictToAbortedWithSchedulerErrorCode() {
    when(facade.pauseJob(any()))
        .thenThrow(
            new SchedulerCommandException(
                "REVISION_CONFLICT", "Expected revision 4 but current revision is 5"));

    StatusRuntimeException exception =
        catchThrowableOfType(
            () ->
                stub.pauseJob(
                    JobMutationRequest.newBuilder()
                        .setRequestId(REQUEST_ID.toString())
                        .setNamespace("billing")
                        .setCaller("grpc-request")
                        .setJobId(JOB_ID.toString())
                        .setExpectedRevision(4)
                        .build()),
            StatusRuntimeException.class);

    assertThat(exception.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(exception.getTrailers()).isNotNull();
    assertThat(
            exception
                .getTrailers()
                .get(Metadata.Key.of("scheduler-error-code", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("REVISION_CONFLICT");
  }

  @Test
  void metadataSuppliesRequestIdAndOverridesCallerIdentity() {
    when(facade.createSchedule(any())).thenReturn(scheduleResult());
    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER),
        METADATA_REQUEST_ID.toString());
    metadata.put(
        Metadata.Key.of("x-mcs-caller", Metadata.ASCII_STRING_MARSHALLER), "metadata-caller");
    var metadataStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

    metadataStub.createSchedule(
        validGrpcRequest().toBuilder().clearRequestId().setCaller("request-caller").build());

    ArgumentCaptor<SchedulerCommands.CreateSchedule> command =
        ArgumentCaptor.forClass(SchedulerCommands.CreateSchedule.class);
    verify(facade).createSchedule(command.capture());
    assertThat(command.getValue().requestId()).isEqualTo(METADATA_REQUEST_ID);
    assertThat(command.getValue().actor()).isEqualTo("metadata-caller");
  }

  @Test
  void protobufTimeMappingDoesNotDependOnJvmDefaultTimezone() {
    TimeZone previous = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Honolulu"));
      GrpcCommandMapper mapper = new GrpcCommandMapper();
      Instant startAt = Instant.parse("2031-04-05T10:15:30.123456789Z");

      SchedulerCommands.CreateTrigger command =
          mapper.createTrigger(
              CreateTriggerRequest.newBuilder()
                  .setRequestId(REQUEST_ID.toString())
                  .setNamespace("billing")
                  .setCaller("grpc-request")
                  .setTrigger(
                      grpcTriggerDraft().toBuilder()
                          .setStartAt(timestamp(startAt))
                          .setSpec(
                              TriggerSpec.newBuilder()
                                  .setSimpleInterval(
                                      SimpleIntervalTrigger.newBuilder()
                                          .setInterval(
                                              Duration.newBuilder().setSeconds(90).setNanos(250))))
                          .build())
                  .build());

      assertThat(command.trigger().startAt()).isEqualTo(startAt);
      assertThat(command.trigger().spec())
          .isEqualTo(new SimpleIntervalTriggerSpec(java.time.Duration.ofSeconds(90, 250), null));
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  private CreateScheduleRequest validGrpcRequest() {
    return CreateScheduleRequest.newBuilder()
        .setRequestId(REQUEST_ID.toString())
        .setNamespace("billing")
        .setCaller("grpc-request")
        .setJob(grpcJobDraft())
        .addTriggers(grpcTriggerDraft())
        .build();
  }

  private JobDraft grpcJobDraft() {
    return JobDraft.newBuilder()
        .setJobId(JOB_ID.toString())
        .setName("invoice-dispatch")
        .setDescription("Dispatch invoice events")
        .setDestinationId(DESTINATION_ID.toString())
        .setDestinationVersion(1)
        .setEventType("billing.invoice.due")
        .setConcurrencyPolicy(CONCURRENCY_POLICY_DISALLOW)
        .setRecoveryPolicy(RECOVERY_POLICY_REQUEST_RECOVERY)
        .setDurable(true)
        .build();
  }

  private TriggerDraft grpcTriggerDraft() {
    return TriggerDraft.newBuilder()
        .setTriggerId(TRIGGER_ID.toString())
        .setJobId(JOB_ID.toString())
        .setName("invoice-cron")
        .setDescription("Daily invoice trigger")
        .setSpec(
            TriggerSpec.newBuilder()
                .setCron(CronTrigger.newBuilder().setExpression("0 0 8 * * ?").setTimezone("UTC")))
        .setStartAt(timestamp(START_AT))
        .setPriority(5)
        .setMisfirePolicy(MISFIRE_POLICY_DO_NOTHING)
        .build();
  }

  private JobMutationRequest jobMutationRequest() {
    return JobMutationRequest.newBuilder()
        .setRequestId(REQUEST_ID.toString())
        .setNamespace("billing")
        .setCaller("grpc-request")
        .setJobId(JOB_ID.toString())
        .setExpectedRevision(1)
        .build();
  }

  private TriggerMutationRequest triggerMutationRequest() {
    return TriggerMutationRequest.newBuilder()
        .setRequestId(REQUEST_ID.toString())
        .setNamespace("billing")
        .setCaller("grpc-request")
        .setTriggerId(TRIGGER_ID.toString())
        .setExpectedRevision(1)
        .build();
  }

  private SchedulerCommands.ScheduleResult scheduleResult() {
    return new SchedulerCommands.ScheduleResult(job(), List.of(trigger()));
  }

  private JobDefinition job() {
    return new JobDefinition(
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
        true,
        JobDefinition.State.ACTIVE,
        1,
        NOW,
        "grpc-test",
        NOW,
        "grpc-test");
  }

  private TriggerDefinition trigger() {
    return new TriggerDefinition(
        TRIGGER_ID,
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
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        NOW,
        "grpc-test",
        NOW,
        "grpc-test");
  }

  private static Timestamp timestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  @EnableAutoConfiguration
  @Import({GrpcCommandMapper.class, GrpcErrorMapper.class})
  static class TestConfig {

    @Bean
    SchedulerCommandFacade schedulerCommandFacade() {
      return mock(SchedulerCommandFacade.class);
    }

    @Bean
    SchedulerCommandGrpcService schedulerCommandGrpcService(
        SchedulerCommandFacade facade, GrpcCommandMapper mapper, GrpcErrorMapper errorMapper) {
      return new SchedulerCommandGrpcService(facade, mapper, errorMapper);
    }
  }
}
