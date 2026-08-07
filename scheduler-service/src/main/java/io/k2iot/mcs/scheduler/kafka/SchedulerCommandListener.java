package io.k2iot.mcs.scheduler.kafka;

import io.k2iot.mcs.scheduler.command.InboxRepository;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class SchedulerCommandListener {

  static final String COMMAND_RESULT_TOPIC = "mcs.scheduler.command-results.v1";

  private final InboxRepository inboxRepository;
  private final KafkaCommandMapper commandMapper;
  private final SchedulerCommandFacade commandFacade;
  private final JsonMapper jsonMapper;
  private final Clock clock;

  public SchedulerCommandListener(
      InboxRepository inboxRepository,
      KafkaCommandMapper commandMapper,
      SchedulerCommandFacade commandFacade,
      JsonMapper jsonMapper,
      Clock clock) {
    this.inboxRepository = Objects.requireNonNull(inboxRepository, "inboxRepository");
    this.commandMapper = Objects.requireNonNull(commandMapper, "commandMapper");
    this.commandFacade = Objects.requireNonNull(commandFacade, "commandFacade");
    this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @KafkaListener(
      topics = "${mcs.scheduler.kafka.command-topic:mcs.scheduler.commands.v1}",
      groupId = "${mcs.scheduler.kafka.consumer-group:mcs-scheduler}")
  @Transactional
  public void onMessage(ConsumerRecord<String, String> record) {
    SchedulerCommandEnvelope envelope = readEnvelope(record.value());
    Instant receivedAt = clock.instant();
    var inboxId =
        inboxRepository.insertIfAbsent(
            new InboxRepository.InboxMessage(
                envelope.messageId(),
                envelope.producer(),
                record.topic(),
                record.partition(),
                record.offset(),
                jsonMapper.valueToTree(envelope),
                readHeaders(record),
                receivedAt));
    if (inboxId.isEmpty()) {
      return;
    }

    KafkaCommandMapper.MappedCommand mapped = commandMapper.map(envelope);
    requireKafkaKey(record.key(), mapped.kafkaKey());
    Object result = execute(mapped);

    UUID resultMessageId = UUID.randomUUID();
    Instant resultOccurredAt = clock.instant();
    Map<String, Object> resultEnvelope = new LinkedHashMap<>();
    resultEnvelope.put("schemaVersion", 1);
    resultEnvelope.put("messageId", resultMessageId.toString());
    resultEnvelope.put("requestId", mapped.requestId().toString());
    resultEnvelope.put("occurredAt", resultOccurredAt.toString());
    resultEnvelope.put("producer", "mcs-scheduler");
    resultEnvelope.put("namespace", mapped.namespace());
    resultEnvelope.put("commandType", mapped.commandType());
    resultEnvelope.put("status", "SUCCEEDED");
    resultEnvelope.put("result", result);
    JsonNode resultPayload = jsonMapper.valueToTree(resultEnvelope);

    inboxRepository.insertCommandResult(
        new InboxRepository.CommandResult(
            resultMessageId,
            mapped.requestId(),
            mapped.namespace(),
            mapped.commandType(),
            resultPayload,
            Map.of("topic", COMMAND_RESULT_TOPIC, "key", mapped.kafkaKey()),
            resultOccurredAt));
    inboxRepository.markCompleted(inboxId.orElseThrow(), clock.instant());
  }

  private Object execute(KafkaCommandMapper.MappedCommand mapped) {
    Object command = mapped.command();
    return switch (mapped.commandType()) {
      case "CREATE_JOB" -> commandFacade.createJob((SchedulerCommands.CreateJob) command);
      case "UPDATE_JOB" -> commandFacade.updateJob((SchedulerCommands.UpdateJob) command);
      case "CREATE_TRIGGER" ->
          commandFacade.createTrigger((SchedulerCommands.CreateTrigger) command);
      case "REPLACE_TRIGGER" ->
          commandFacade.replaceTrigger((SchedulerCommands.ReplaceTrigger) command);
      case "CREATE_SCHEDULE" ->
          commandFacade.createSchedule((SchedulerCommands.CreateSchedule) command);
      case "PAUSE_JOB" -> commandFacade.pauseJob((SchedulerCommands.JobMutation) command);
      case "RESUME_JOB" -> commandFacade.resumeJob((SchedulerCommands.JobMutation) command);
      case "DELETE_JOB" -> commandFacade.deleteJob((SchedulerCommands.JobMutation) command);
      case "PAUSE_TRIGGER" ->
          commandFacade.pauseTrigger((SchedulerCommands.TriggerMutation) command);
      case "RESUME_TRIGGER" ->
          commandFacade.resumeTrigger((SchedulerCommands.TriggerMutation) command);
      case "DELETE_TRIGGER" ->
          commandFacade.deleteTrigger((SchedulerCommands.TriggerMutation) command);
      case "FIRE_TRIGGER_NOW" ->
          commandFacade.fireTriggerNow((SchedulerCommands.FireTriggerNow) command);
      default ->
          throw new KafkaCommandMapper.KafkaCommandException(
              "UNKNOWN_COMMAND_TYPE", "Unsupported mapped command: " + mapped.commandType());
    };
  }

  private SchedulerCommandEnvelope readEnvelope(String value) {
    try {
      return jsonMapper.readValue(value, SchedulerCommandEnvelope.class);
    } catch (JacksonException | IllegalArgumentException exception) {
      throw new KafkaCommandMapper.KafkaCommandException(
          "INVALID_ENVELOPE", "Kafka command envelope is not valid JSON", exception);
    }
  }

  private static void requireKafkaKey(String actual, String expected) {
    if (!Objects.equals(actual, expected)) {
      throw new KafkaCommandMapper.KafkaCommandException(
          "INVALID_COMMAND_KEY", "Kafka key must be " + expected);
    }
  }

  private static Map<String, String> readHeaders(ConsumerRecord<String, String> record) {
    Map<String, String> headers = new LinkedHashMap<>();
    for (Header header : record.headers()) {
      byte[] value = header.value();
      headers.put(header.key(), value == null ? "" : new String(value, StandardCharsets.UTF_8));
    }
    return Map.copyOf(headers);
  }
}
