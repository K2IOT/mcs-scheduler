package io.k2iot.mcs.scheduler.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.JobDraft;
import io.k2iot.mcs.scheduler.v1.RecoveryPolicy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaSchedulerCommandPublisherTest {

  @Test
  void generatesMissingRequestIdOncePerLogicalCall() {
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    UUID generatedRequestId = UUID.fromString("73000000-0000-4000-8000-000000000001");
    UUID messageId = UUID.fromString("74000000-0000-4000-8000-000000000001");
    var calls = new int[1];
    Supplier<UUID> requestIds =
        () -> {
          calls[0]++;
          return generatedRequestId;
        };
    var publisher = publisher(kafkaTemplate, requestIds, () -> messageId);

    CommandReceipt receipt = publisher.createSchedule(validScheduleRequest(), null);

    assertThat(calls[0]).isEqualTo(1);
    assertThat(receipt.requestId()).isEqualTo(generatedRequestId);
    ArgumentCaptor<String> envelope = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate).send(anyString(), anyString(), envelope.capture());
    assertThat(envelope.getValue()).contains("\"requestId\":\"" + generatedRequestId + "\"");
  }

  @Test
  void mapsCreateScheduleToKafkaV1PayloadShape() {
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    UUID requestId = UUID.fromString("75000000-0000-4000-8000-000000000001");
    UUID jobId = UUID.fromString("75000000-0000-4000-8000-000000000002");
    UUID destinationId = UUID.fromString("75000000-0000-4000-8000-000000000003");
    var publisher = publisher(kafkaTemplate, UUID::randomUUID, UUID::randomUUID);
    CreateScheduleRequest request =
        CreateScheduleRequest.newBuilder()
            .setNamespace("billing")
            .setCaller("billing-service")
            .setJob(
                JobDraft.newBuilder()
                    .setJobId(jobId.toString())
                    .setName("invoice-sweep")
                    .setDestinationId(destinationId.toString())
                    .setDestinationVersion(3)
                    .setEventType("billing.invoice.sweep")
                    .setConcurrencyPolicy(ConcurrencyPolicy.CONCURRENCY_POLICY_DISALLOW)
                    .setRecoveryPolicy(RecoveryPolicy.RECOVERY_POLICY_NONE)
                    .setDurable(true))
            .build();

    publisher.createSchedule(request, requestId);

    ArgumentCaptor<String> envelope = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate).send(anyString(), anyString(), envelope.capture());
    assertThat(envelope.getValue())
        .contains("\"job\":{\"jobId\":\"" + jobId + "\",\"namespace\":\"billing\"")
        .contains("\"concurrencyPolicy\":\"DISALLOW\"")
        .contains("\"recoveryPolicy\":\"NONE\"");
  }

  @Test
  void rejectsCascadeDeleteUnsupportedBySchedulerV1() {
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    var publisher = publisher(kafkaTemplate, UUID::randomUUID, UUID::randomUUID);

    assertThatThrownBy(
            () ->
                publisher.deleteJob(
                    UUID.fromString("76000000-0000-4000-8000-000000000001"),
                    "billing",
                    1,
                    true,
                    UUID.fromString("76000000-0000-4000-8000-000000000002")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cascade");
    verifyNoInteractions(kafkaTemplate);
  }

  private CreateScheduleRequest validScheduleRequest() {
    return CreateScheduleRequest.newBuilder()
        .setNamespace("billing")
        .setCaller("billing-service")
        .setJob(
            JobDraft.newBuilder()
                .setJobId("73000000-0000-4000-8000-000000000002")
                .setName("invoice-sweep")
                .setDestinationId("73000000-0000-4000-8000-000000000003")
                .setDestinationVersion(1)
                .setEventType("billing.invoice.sweep")
                .setConcurrencyPolicy(ConcurrencyPolicy.CONCURRENCY_POLICY_ALLOW)
                .setRecoveryPolicy(RecoveryPolicy.RECOVERY_POLICY_NONE))
        .build();
  }

  private KafkaSchedulerCommandPublisher publisher(
      KafkaTemplate<String, String> kafkaTemplate,
      Supplier<UUID> requestIds,
      Supplier<UUID> messageIds) {
    return new KafkaSchedulerCommandPublisher(
        kafkaTemplate,
        "scheduler.commands.v1",
        "billing-service",
        requestIds,
        messageIds,
        Clock.fixed(Instant.parse("2026-08-08T06:00:00Z"), ZoneOffset.UTC));
  }
}
