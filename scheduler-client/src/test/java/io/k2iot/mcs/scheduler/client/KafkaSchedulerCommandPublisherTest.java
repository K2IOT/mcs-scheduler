package io.k2iot.mcs.scheduler.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
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
    var publisher =
        new KafkaSchedulerCommandPublisher(
            kafkaTemplate,
            "scheduler.commands.v1",
            "billing-service",
            requestIds,
            () -> messageId,
            Clock.fixed(Instant.parse("2026-08-08T06:00:00Z"), ZoneOffset.UTC));

    CommandReceipt receipt =
        publisher.createSchedule(
            CreateScheduleRequest.newBuilder()
                .setNamespace("billing")
                .setCaller("billing-service")
                .build(),
            null);

    assertThat(calls[0]).isEqualTo(1);
    assertThat(receipt.requestId()).isEqualTo(generatedRequestId);
    ArgumentCaptor<String> envelope = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate).send(anyString(), anyString(), envelope.capture());
    assertThat(envelope.getValue()).contains("\"requestId\":\"" + generatedRequestId + "\"");
  }
}
