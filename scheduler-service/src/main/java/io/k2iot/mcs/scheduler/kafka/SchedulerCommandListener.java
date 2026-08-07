package io.k2iot.mcs.scheduler.kafka;

import io.k2iot.mcs.scheduler.command.InboxRepository;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class SchedulerCommandListener {

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
                Map.of(),
                receivedAt));
    if (inboxId.isEmpty()) {
      return;
    }

    KafkaCommandMapper.MappedCommand mapped = commandMapper.map(envelope);
    Object result = execute(mapped.command());
    JsonNode resultPayload =
        jsonMapper.valueToTree(
            Map.of(
                "schemaVersion", 1,
                "messageId", UUID.randomUUID().toString(),
                "requestId", mapped.requestId().toString(),
                "occurredAt", clock.instant().toString(),
                "producer", "mcs-scheduler",
                "namespace", mapped.namespace(),
                "commandType", mapped.commandType(),
                "status", "SUCCEEDED",
                "result", result));
    inboxRepository.insertCommandResult(
        new InboxRepository.CommandResult(
            UUID.randomUUID(),
            mapped.requestId(),
            mapped.namespace(),
            mapped.commandType(),
            resultPayload,
            Map.of("topic", "mcs.scheduler.command-results.v1"),
            clock.instant()));
    inboxRepository.markCompleted(inboxId.orElseThrow(), clock.instant());
  }

  private Object execute(Object command) {
    if (command instanceof SchedulerCommands.CreateSchedule createSchedule) {
      return commandFacade.createSchedule(createSchedule);
    }
    throw new KafkaCommandMapper.KafkaCommandException(
        "UNKNOWN_COMMAND_TYPE", "Unsupported mapped command: " + command.getClass().getName());
  }

  private SchedulerCommandEnvelope readEnvelope(String value) {
    try {
      return jsonMapper.readValue(value, SchedulerCommandEnvelope.class);
    } catch (JacksonException exception) {
      throw new KafkaCommandMapper.KafkaCommandException(
          "INVALID_ENVELOPE", "Kafka command envelope is not valid JSON", exception);
    }
  }
}
