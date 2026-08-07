package io.k2iot.mcs.scheduler.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.command.InboxRepository;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class SchedulerCommandListenerTest {

  private static final Instant NOW = Instant.parse("2026-08-07T04:00:00Z");
  private static final UUID MESSAGE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID REQUEST_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
  private static final UUID INBOX_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");

  @Test
  void duplicateMessageIdDoesNotExecuteCommandTwice() throws Exception {
    InboxRepository inboxRepository = mock(InboxRepository.class);
    KafkaCommandMapper commandMapper = mock(KafkaCommandMapper.class);
    SchedulerCommandFacade commandFacade = mock(SchedulerCommandFacade.class);
    JsonMapper jsonMapper = JsonMapper.builder().build();
    SchedulerCommands.CreateSchedule command = mock(SchedulerCommands.CreateSchedule.class);
    SchedulerCommands.ScheduleResult result = mock(SchedulerCommands.ScheduleResult.class);

    when(inboxRepository.insertIfAbsent(any()))
        .thenReturn(Optional.of(INBOX_ID), Optional.empty());
    when(commandMapper.map(any()))
        .thenReturn(
            new KafkaCommandMapper.MappedCommand(
                "CREATE_SCHEDULE", REQUEST_ID, "billing", REQUEST_ID, command));
    when(commandFacade.createSchedule(command)).thenReturn(result);

    SchedulerCommandListener listener =
        new SchedulerCommandListener(
            inboxRepository,
            commandMapper,
            commandFacade,
            jsonMapper,
            Clock.fixed(NOW, ZoneOffset.UTC));

    String value =
        jsonMapper.writeValueAsString(
            new SchedulerCommandEnvelope(
                1,
                MESSAGE_ID.toString(),
                REQUEST_ID.toString(),
                NOW,
                "billing-service",
                "billing",
                "CREATE_SCHEDULE",
                jsonMapper.createObjectNode()));
    ConsumerRecord<String, String> record =
        new ConsumerRecord<>("mcs.scheduler.commands.v1", 0, 42L, "billing:" + REQUEST_ID, value);

    listener.onMessage(record);
    listener.onMessage(record);

    verify(commandFacade, times(1)).createSchedule(command);
  }
}
