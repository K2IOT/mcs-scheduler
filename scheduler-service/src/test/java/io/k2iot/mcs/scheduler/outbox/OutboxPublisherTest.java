package io.k2iot.mcs.scheduler.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaOperations;
import tools.jackson.databind.json.JsonMapper;

class OutboxPublisherTest extends PostgresIntegrationTestBase {

  private static final Instant NOW = Instant.parse("2026-08-07T12:00:00Z");
  private static final UUID EVENT_ID = UUID.fromString("71000000-0000-4000-8000-000000000001");
  private static final UUID AGGREGATE_ID = UUID.fromString("72000000-0000-4000-8000-000000000001");

  @Autowired JdbcTemplate jdbc;
  @Autowired JdbcClient jdbcClient;
  @Autowired JsonMapper jsonMapper;

  @BeforeEach
  void cleanBefore() {
    cleanState();
  }

  @AfterEach
  void cleanAfter() {
    cleanState();
  }

  @Test
  void concurrentClaimersReturnPendingEventToOnlyOneWorker() throws Exception {
    insertPendingOutbox(EVENT_ID, NOW);
    var repository = new JdbcOutboxClaimRepository(jdbcClient, jsonMapper);
    var ready = new CountDownLatch(2);
    var start = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(2);

    try {
      var first =
          executor.submit(() -> claimAfterBarrier(repository, ready, start, UUID.randomUUID()));
      var second =
          executor.submit(() -> claimAfterBarrier(repository, ready, start, UUID.randomUUID()));
      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      start.countDown();

      List<OutboxClaimRepository.ClaimedOutboxEvent> claimed =
          Stream.concat(
                  first.get(10, TimeUnit.SECONDS).stream(),
                  second.get(10, TimeUnit.SECONDS).stream())
              .toList();

      assertThat(claimed)
          .extracting(OutboxClaimRepository.ClaimedOutboxEvent::eventId)
          .containsExactly(EVENT_ID);
      assertThat(
              jdbc.queryForObject(
                  "select state from scheduler.outbox_event where outbox_event_id = ?",
                  String.class,
                  EVENT_ID))
          .isEqualTo("IN_PROGRESS");
      assertThat(
              jdbc.queryForObject(
                  "select publish_attempts from scheduler.outbox_event where outbox_event_id = ?",
                  Integer.class,
                  EVENT_ID))
          .isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void failedPublishReschedulesWithFirstBackoff() throws Exception {
    insertPendingOutbox(EVENT_ID, NOW);
    var repository = new JdbcOutboxClaimRepository(jdbcClient, jsonMapper);
    KafkaOperations<Object, Object> kafka = failingKafka("broker unavailable");
    var properties = new OutboxProperties();
    var metrics = new SimpleMeterRegistry();
    var publisher =
        new OutboxPublisher(
            repository, kafka, properties, Clock.fixed(NOW, ZoneOffset.UTC), metrics);

    publisher.publishOnce();

    assertThat(outboxState()).isEqualTo("PENDING");
    assertThat(publishAttempts()).isEqualTo(1);
    assertThat(nextAttemptAt()).isEqualTo(NOW.plusSeconds(1));
    assertThat(lastError()).contains("broker unavailable");
    assertThat(metrics.counter("mcs.scheduler.outbox.dead").count()).isZero();
    verify(kafka).send(any(ProducerRecord.class));
  }

  @Test
  void twentiethFailedAttemptMarksEventDeadAndEmitsMetric() throws Exception {
    insertPendingOutbox(EVENT_ID, NOW);
    jdbc.update(
        "update scheduler.outbox_event set publish_attempts = 19 where outbox_event_id = ?",
        EVENT_ID);
    var repository = new JdbcOutboxClaimRepository(jdbcClient, jsonMapper);
    KafkaOperations<Object, Object> kafka = failingKafka("broker unavailable");
    var properties = new OutboxProperties();
    var metrics = new SimpleMeterRegistry();
    var publisher =
        new OutboxPublisher(
            repository, kafka, properties, Clock.fixed(NOW, ZoneOffset.UTC), metrics);

    publisher.publishOnce();

    assertThat(outboxState()).isEqualTo("DEAD");
    assertThat(publishAttempts()).isEqualTo(20);
    assertThat(nextAttemptAt()).isNull();
    assertThat(lastError()).contains("broker unavailable");
    assertThat(metrics.counter("mcs.scheduler.outbox.dead").count()).isEqualTo(1.0);
  }

  private List<OutboxClaimRepository.ClaimedOutboxEvent> claimAfterBarrier(
      OutboxClaimRepository repository, CountDownLatch ready, CountDownLatch start, UUID claimId)
      throws InterruptedException {
    ready.countDown();
    if (!start.await(5, TimeUnit.SECONDS)) {
      throw new IllegalStateException("claim workers did not start together");
    }
    return repository.claimBatch(100, NOW, Duration.ofSeconds(30), claimId);
  }

  @SuppressWarnings("unchecked")
  private KafkaOperations<Object, Object> failingKafka(String message) {
    KafkaOperations<Object, Object> kafka = mock(KafkaOperations.class);
    when(kafka.send(any(ProducerRecord.class)))
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException(message)));
    return kafka;
  }

  private void insertPendingOutbox(UUID eventId, Instant occurredAt) {
    jdbc.update(
        """
        insert into scheduler.outbox_event (
            outbox_event_id, aggregate_type, aggregate_id, event_type,
            payload, headers, occurred_at, published_at, publish_attempts, last_error)
        values (?, 'COMMAND_REQUEST', ?, 'SCHEDULER_COMMAND_RESULT',
                '{"schemaVersion":1,"namespace":"billing"}'::jsonb,
                '{"topic":"mcs.scheduler.command-results.v1","key":"billing:request"}'::jsonb,
                ?, null, 0, null)
        """,
        eventId,
        AGGREGATE_ID,
        occurredAt.atOffset(ZoneOffset.UTC));
  }

  private String outboxState() {
    return jdbc.queryForObject(
        "select state from scheduler.outbox_event where outbox_event_id = ?",
        String.class,
        EVENT_ID);
  }

  private int publishAttempts() {
    return jdbc.queryForObject(
        "select publish_attempts from scheduler.outbox_event where outbox_event_id = ?",
        Integer.class,
        EVENT_ID);
  }

  private Instant nextAttemptAt() {
    OffsetDateTime value =
        jdbc.queryForObject(
            "select next_attempt_at from scheduler.outbox_event where outbox_event_id = ?",
            OffsetDateTime.class,
            EVENT_ID);
    return value == null ? null : value.toInstant();
  }

  private String lastError() {
    return jdbc.queryForObject(
        "select last_error from scheduler.outbox_event where outbox_event_id = ?",
        String.class,
        EVENT_ID);
  }

  private void cleanState() {
    jdbc.update("delete from scheduler.outbox_event");
  }
}
