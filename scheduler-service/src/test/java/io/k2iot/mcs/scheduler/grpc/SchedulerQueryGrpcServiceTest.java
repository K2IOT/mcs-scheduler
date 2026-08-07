package io.k2iot.mcs.scheduler.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import io.k2iot.mcs.scheduler.v1.GetExecutionRequest;
import io.k2iot.mcs.scheduler.v1.GetJobRequest;
import io.k2iot.mcs.scheduler.v1.GetTriggerRequest;
import io.k2iot.mcs.scheduler.v1.ListJobTriggersRequest;
import io.k2iot.mcs.scheduler.v1.SchedulerQueryServiceGrpc;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureInProcessTransport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(SchedulerQueryGrpcServiceTest.TestConfig.class)
@AutoConfigureInProcessTransport
@TestPropertySource(
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,org.springframework.boot.quartz.autoconfigure.QuartzAutoConfiguration,org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration,org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
    })
class SchedulerQueryGrpcServiceTest {

  private static final Instant NOW = Instant.parse("2026-08-07T03:00:00Z");
  private static final UUID JOB_ID = UUID.fromString("20000000-0000-4000-8000-000000000018");
  private static final UUID TRIGGER_ID = UUID.fromString("30000000-0000-4000-8000-000000000018");
  private static final UUID DESTINATION_ID =
      UUID.fromString("40000000-0000-4000-8000-000000000018");

  @Autowired JobRepository jobRepository;
  @Autowired TriggerRepository triggerRepository;
  @Autowired GrpcChannelFactory channelFactory;

  private SchedulerQueryServiceGrpc.SchedulerQueryServiceBlockingStub stub;

  @BeforeEach
  void setUp() {
    reset(jobRepository, triggerRepository);
    stub = SchedulerQueryServiceGrpc.newBlockingStub(channelFactory.createChannel("localhost:9090"));
  }

  @Test
  void readsJobAndTriggerQueriesFromExistingRepositories() {
    when(jobRepository.findById(JOB_ID)).thenReturn(Optional.of(job()));
    when(triggerRepository.findById(TRIGGER_ID)).thenReturn(Optional.of(trigger()));
    when(triggerRepository.findByJobId(JOB_ID)).thenReturn(List.of(trigger()));

    var jobResponse =
        stub.getJob(
            GetJobRequest.newBuilder()
                .setNamespace("billing")
                .setJobId(JOB_ID.toString())
                .build());
    var triggerResponse =
        stub.getTrigger(
            GetTriggerRequest.newBuilder()
                .setNamespace("billing")
                .setTriggerId(TRIGGER_ID.toString())
                .build());
    var listResponse =
        stub.listJobTriggers(
            ListJobTriggersRequest.newBuilder()
                .setNamespace("billing")
                .setJobId(JOB_ID.toString())
                .setPageSize(100)
                .build());

    assertThat(jobResponse.getJob().getJobId()).isEqualTo(JOB_ID.toString());
    assertThat(triggerResponse.getTrigger().getTriggerId()).isEqualTo(TRIGGER_ID.toString());
    assertThat(listResponse.getTriggersCount()).isEqualTo(1);
  }

  @Test
  void executionQueriesRemainUnimplementedUntilTask10ExecutionRepositoryExists() {
    StatusRuntimeException exception =
        catchThrowableOfType(
            () ->
                stub.getExecution(
                    GetExecutionRequest.newBuilder()
                        .setNamespace("billing")
                        .setExecutionId(UUID.randomUUID().toString())
                        .build()),
            StatusRuntimeException.class);

    assertThat(exception.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
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
        Instant.parse("2030-01-01T00:00:00Z"),
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

  @EnableAutoConfiguration
  @Import({GrpcCommandMapper.class, GrpcErrorMapper.class})
  static class TestConfig {

    @Bean
    JobRepository jobRepository() {
      return mock(JobRepository.class);
    }

    @Bean
    TriggerRepository triggerRepository() {
      return mock(TriggerRepository.class);
    }

    @Bean
    SchedulerQueryGrpcService schedulerQueryGrpcService(
        JobRepository jobRepository,
        TriggerRepository triggerRepository,
        GrpcCommandMapper mapper,
        GrpcErrorMapper errorMapper) {
      return new SchedulerQueryGrpcService(jobRepository, triggerRepository, mapper, errorMapper);
    }
  }
}
