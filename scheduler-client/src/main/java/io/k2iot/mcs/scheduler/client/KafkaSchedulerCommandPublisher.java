package io.k2iot.mcs.scheduler.client;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.kafka.core.KafkaTemplate;

public final class KafkaSchedulerCommandPublisher implements AsyncSchedulerClient {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;
  private final String producer;
  private final Supplier<UUID> requestIds;
  private final Supplier<UUID> messageIds;
  private final Clock clock;
  private final JsonFormat.Printer jsonPrinter = JsonFormat.printer().omittingInsignificantWhitespace();

  public KafkaSchedulerCommandPublisher(
      KafkaTemplate<String, String> kafkaTemplate, String topic, String producer) {
    this(kafkaTemplate, topic, producer, UUID::randomUUID, UUID::randomUUID, Clock.systemUTC());
  }

  KafkaSchedulerCommandPublisher(
      KafkaTemplate<String, String> kafkaTemplate,
      String topic,
      String producer,
      Supplier<UUID> requestIds,
      Supplier<UUID> messageIds,
      Clock clock) {
    this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
    this.topic = requireText(topic, "topic");
    this.producer = requireText(producer, "producer");
    this.requestIds = Objects.requireNonNull(requestIds, "requestIds");
    this.messageIds = Objects.requireNonNull(messageIds, "messageIds");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public CommandReceipt createSchedule(CreateScheduleRequest request, UUID requestId) {
    Objects.requireNonNull(request, "request");
    String namespace = requireText(request.getNamespace(), "namespace");
    String payload =
        "{\"job\":"
            + json(request.getJob())
            + ",\"triggers\":["
            + request.getTriggersList().stream().map(this::json).reduce((a, b) -> a + "," + b).orElse("")
            + "]}";
    return publish(
        "CREATE_SCHEDULE",
        namespace,
        request.getCaller().isBlank() ? producer : request.getCaller(),
        payload,
        aggregateKey(namespace, request.getJob().getJobId()),
        requestId);
  }

  @Override
  public CommandReceipt pauseJob(
      UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    return publishJobMutation("PAUSE_JOB", jobId, namespace, expectedRevision, requestId, false);
  }

  @Override
  public CommandReceipt resumeJob(
      UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    return publishJobMutation("RESUME_JOB", jobId, namespace, expectedRevision, requestId, false);
  }

  @Override
  public CommandReceipt deleteJob(
      UUID jobId, String namespace, long expectedRevision, boolean cascade, UUID requestId) {
    return publishJobMutation("DELETE_JOB", jobId, namespace, expectedRevision, requestId, cascade);
  }

  @Override
  public CommandReceipt fireTrigger(
      UUID triggerId, String namespace, UUID manualFireId, UUID requestId) {
    Objects.requireNonNull(triggerId, "triggerId");
    Objects.requireNonNull(manualFireId, "manualFireId");
    String normalizedNamespace = requireText(namespace, "namespace");
    String payload =
        "{\"triggerId\":\""
            + triggerId
            + "\",\"manualFireId\":\""
            + manualFireId
            + "\"}";
    return publish(
        "FIRE_TRIGGER_NOW",
        normalizedNamespace,
        producer,
        payload,
        normalizedNamespace + ":" + triggerId,
        requestId);
  }

  private CommandReceipt publishJobMutation(
      String commandType,
      UUID jobId,
      String namespace,
      long expectedRevision,
      UUID requestId,
      boolean cascade) {
    Objects.requireNonNull(jobId, "jobId");
    String normalizedNamespace = requireText(namespace, "namespace");
    long revision = positiveRevision(expectedRevision);
    String payload =
        "{\"jobId\":\""
            + jobId
            + "\",\"expectedRevision\":"
            + revision
            + ("DELETE_JOB".equals(commandType) ? ",\"cascade\":" + cascade : "")
            + "}";
    return publish(
        commandType,
        normalizedNamespace,
        producer,
        payload,
        normalizedNamespace + ":" + jobId,
        requestId);
  }

  private CommandReceipt publish(
      String commandType,
      String namespace,
      String caller,
      String payload,
      String key,
      UUID suppliedRequestId) {
    UUID effectiveRequestId =
        suppliedRequestId != null
            ? suppliedRequestId
            : Objects.requireNonNull(requestIds.get(), "requestId");
    UUID messageId = Objects.requireNonNull(messageIds.get(), "messageId");
    Instant occurredAt = clock.instant();
    String envelope =
        "{\"schemaVersion\":1,\"messageId\":\""
            + messageId
            + "\",\"requestId\":\""
            + effectiveRequestId
            + "\",\"occurredAt\":\""
            + occurredAt
            + "\",\"producer\":\""
            + escape(requireText(caller, "caller"))
            + "\",\"namespace\":\""
            + escape(namespace)
            + "\",\"commandType\":\""
            + commandType
            + "\",\"payload\":"
            + payload
            + "}";
    kafkaTemplate.send(topic, key, envelope);
    return new CommandReceipt(effectiveRequestId, messageId, topic);
  }

  private String json(MessageOrBuilder value) {
    try {
      return jsonPrinter.print(value);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Unable to serialize scheduler command payload", exception);
    }
  }

  private String aggregateKey(String namespace, String aggregateId) {
    if (aggregateId != null && !aggregateId.isBlank()) {
      return namespace + ":" + aggregateId;
    }
    return namespace + ":schedule";
  }

  private static String escape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static long positiveRevision(long revision) {
    if (revision < 1) {
      throw new IllegalArgumentException("expectedRevision must be positive");
    }
    return revision;
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
