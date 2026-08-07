package io.k2iot.mcs.scheduler.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.Scheduled;
import tools.jackson.databind.JsonNode;

public class OutboxPublisher {

  private static final List<Duration> RETRY_DELAYS =
      List.of(
          Duration.ofSeconds(1),
          Duration.ofSeconds(5),
          Duration.ofSeconds(30),
          Duration.ofMinutes(2),
          Duration.ofMinutes(10));
  private static final List<String> APPROVED_KEY_VARIABLES =
      List.of("jobId", "triggerId", "executionId", "namespace");

  private final OutboxClaimRepository repository;
  private final KafkaOperations<Object, Object> kafka;
  private final OutboxProperties properties;
  private final Clock clock;
  private final Counter deadCounter;

  public OutboxPublisher(
      OutboxClaimRepository repository,
      KafkaOperations<Object, Object> kafka,
      OutboxProperties properties,
      Clock clock,
      MeterRegistry meterRegistry) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.kafka = Objects.requireNonNull(kafka, "kafka");
    this.properties = Objects.requireNonNull(properties, "properties");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.deadCounter =
        Objects.requireNonNull(meterRegistry, "meterRegistry").counter("mcs.scheduler.outbox.dead");
  }

  @Scheduled(fixedDelayString = "${mcs.scheduler.outbox.poll-interval:PT1S}")
  public void scheduledPublish() {
    if (properties.isEnabled()) {
      publishOnce();
    }
  }

  public void publishOnce() {
    Instant now = clock.instant();
    UUID claimId = UUID.randomUUID();
    List<OutboxClaimRepository.ClaimedOutboxEvent> events =
        repository.claimBatch(
            properties.getBatchSize(), now, properties.getClaimTimeout(), claimId);
    for (OutboxClaimRepository.ClaimedOutboxEvent event : events) {
      if (isExpired(event, now)) {
        markDead(event, now, "Outbox event exceeded maximum delivery age");
        continue;
      }
      try {
        kafka.send(recordFor(event))
            .get(properties.getPublishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        repository.markPublished(event.eventId(), event.claimId(), clock.instant());
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        handleFailure(event, exception, clock.instant());
        return;
      } catch (Exception exception) {
        handleFailure(event, exception, clock.instant());
      }
    }
  }

  private ProducerRecord<Object, Object> recordFor(
      OutboxClaimRepository.ClaimedOutboxEvent event) {
    String topic = requiredHeader(event, "topic");
    String defaultKey = event.headers().getOrDefault("key", event.aggregateId().toString());
    String key = resolveKeyExpression(event, defaultKey);
    return new ProducerRecord<>(topic, key, event.payload().toString());
  }

  private String resolveKeyExpression(
      OutboxClaimRepository.ClaimedOutboxEvent event, String defaultKey) {
    String expression = event.headers().get("keyExpression");
    if (expression == null || expression.isBlank()) {
      return defaultKey;
    }

    String resolved = expression;
    for (String variable : APPROVED_KEY_VARIABLES) {
      String token = "${" + variable + "}";
      if (!resolved.contains(token)) {
        continue;
      }
      JsonNode value = event.payload().get(variable);
      if (value == null || value.isNull() || !value.isValueNode()) {
        return defaultKey;
      }
      resolved = resolved.replace(token, value.asText());
    }
    return resolved.contains("${") || resolved.isBlank() ? defaultKey : resolved;
  }

  private String requiredHeader(OutboxClaimRepository.ClaimedOutboxEvent event, String name) {
    String value = event.headers().get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Outbox event is missing routing header: " + name);
    }
    return value;
  }

  private void handleFailure(
      OutboxClaimRepository.ClaimedOutboxEvent event, Throwable throwable, Instant failedAt) {
    String error = failureMessage(throwable);
    if (event.publishAttempts() >= properties.getMaxAttempts()
        || isExpired(event, failedAt)) {
      markDead(event, failedAt, error);
      return;
    }
    repository.reschedule(
        event.eventId(),
        event.claimId(),
        failedAt.plus(retryDelay(event.publishAttempts())),
        error);
  }

  private void markDead(
      OutboxClaimRepository.ClaimedOutboxEvent event, Instant failedAt, String error) {
    repository.markDead(event.eventId(), event.claimId(), failedAt, error);
    deadCounter.increment();
  }

  private boolean isExpired(OutboxClaimRepository.ClaimedOutboxEvent event, Instant now) {
    return !now.isBefore(event.occurredAt().plus(properties.getMaxAge()));
  }

  private static Duration retryDelay(int publishAttempts) {
    int index = Math.min(Math.max(publishAttempts, 1) - 1, RETRY_DELAYS.size() - 1);
    return RETRY_DELAYS.get(index);
  }

  private static String failureMessage(Throwable throwable) {
    Throwable cause = throwable;
    if (throwable instanceof ExecutionException executionException
        && executionException.getCause() != null) {
      cause = executionException.getCause();
    }
    String message = cause.getMessage();
    return message == null || message.isBlank() ? cause.getClass().getName() : message;
  }
}
