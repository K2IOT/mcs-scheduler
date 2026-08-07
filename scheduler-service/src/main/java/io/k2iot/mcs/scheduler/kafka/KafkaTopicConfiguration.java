package io.k2iot.mcs.scheduler.kafka;

import io.k2iot.mcs.scheduler.command.InboxRepository;
import io.k2iot.mcs.scheduler.command.JdbcInboxRepository;
import io.k2iot.mcs.scheduler.command.SchedulerCommandAutoConfiguration;
import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.outbox.OutboxClaimRepository;
import io.k2iot.mcs.scheduler.outbox.OutboxProperties;
import io.k2iot.mcs.scheduler.outbox.OutboxPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(
    after = SchedulerCommandAutoConfiguration.class,
    afterName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@ConditionalOnBean(KafkaOperations.class)
@EnableConfigurationProperties(OutboxProperties.class)
@EnableScheduling
public class KafkaTopicConfiguration {

  static final String MESSAGE_ID_HEADER = "mcs.scheduler.messageId";
  static final String REQUEST_ID_HEADER = "mcs.scheduler.requestId";
  static final String ERROR_CODE_HEADER = "mcs.scheduler.errorCode";

  @Bean
  InboxRepository schedulerInboxRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
    return new JdbcInboxRepository(jdbc, jsonMapper);
  }

  @Bean
  SchedulerCommandListener schedulerCommandListener(
      InboxRepository inboxRepository,
      KafkaCommandMapper commandMapper,
      SchedulerCommandFacade commandFacade,
      JsonMapper jsonMapper,
      Clock clock,
      @Value("${mcs.scheduler.kafka.command-result-topic:mcs.scheduler.command-results.v1}")
          String commandResultTopic) {
    return new SchedulerCommandListener(
        inboxRepository, commandMapper, commandFacade, jsonMapper, clock, commandResultTopic);
  }

  @Bean
  OutboxPublisher schedulerOutboxPublisher(
      OutboxClaimRepository outboxClaimRepository,
      KafkaOperations<Object, Object> kafkaOperations,
      OutboxProperties properties,
      Clock clock,
      MeterRegistry meterRegistry) {
    return new OutboxPublisher(
        outboxClaimRepository, kafkaOperations, properties, clock, meterRegistry);
  }

  @Bean
  NewTopic schedulerCommandTopic(
      @Value("${mcs.scheduler.kafka.command-topic:mcs.scheduler.commands.v1}") String topic,
      @Value("${mcs.scheduler.kafka.partitions:6}") int partitions,
      @Value("${mcs.scheduler.kafka.replicas:1}") int replicas) {
    return TopicBuilder.name(topic).partitions(partitions).replicas(replicas).build();
  }

  @Bean
  NewTopic schedulerCommandResultTopic(
      @Value("${mcs.scheduler.kafka.command-result-topic:mcs.scheduler.command-results.v1}")
          String topic,
      @Value("${mcs.scheduler.kafka.partitions:6}") int partitions,
      @Value("${mcs.scheduler.kafka.replicas:1}") int replicas) {
    return TopicBuilder.name(topic).partitions(partitions).replicas(replicas).build();
  }

  @Bean
  NewTopic schedulerCommandDltTopic(
      @Value("${mcs.scheduler.kafka.dlt-topic:mcs.scheduler.commands.v1.DLT}") String topic,
      @Value("${mcs.scheduler.kafka.partitions:6}") int partitions,
      @Value("${mcs.scheduler.kafka.replicas:1}") int replicas) {
    return TopicBuilder.name(topic).partitions(partitions).replicas(replicas).build();
  }

  @Bean
  CommonErrorHandler schedulerKafkaErrorHandler(
      KafkaOperations<Object, Object> kafkaOperations,
      JsonMapper jsonMapper,
      @Value("${mcs.scheduler.kafka.dlt-topic:mcs.scheduler.commands.v1.DLT}") String dltTopic,
      @Value("${mcs.scheduler.kafka.retry-backoff-ms:250}") long retryBackoffMs,
      @Value("${mcs.scheduler.kafka.retry-attempts:2}") long retryAttempts) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            kafkaOperations,
            (record, exception) -> new TopicPartition(dltTopic, record.partition()));
    recoverer.addHeadersFunction(
        (record, exception) -> commandHeaders(record, exception, jsonMapper));
    recoverer.setFailIfSendResultIsError(true);

    DefaultErrorHandler errorHandler =
        new DefaultErrorHandler(recoverer, new FixedBackOff(retryBackoffMs, retryAttempts));
    errorHandler.addNotRetryableExceptions(KafkaCommandMapper.KafkaCommandException.class);
    return errorHandler;
  }

  private static RecordHeaders commandHeaders(
      ConsumerRecord<?, ?> record, Exception exception, JsonMapper jsonMapper) {
    RecordHeaders headers = new RecordHeaders();
    SchedulerCommandEnvelope envelope = readEnvelope(record.value(), jsonMapper);
    if (envelope != null) {
      addHeader(headers, MESSAGE_ID_HEADER, envelope.messageId());
      addHeader(headers, REQUEST_ID_HEADER, envelope.requestId());
    }
    addHeader(headers, ERROR_CODE_HEADER, errorCode(exception));
    return headers;
  }

  private static SchedulerCommandEnvelope readEnvelope(Object value, JsonMapper jsonMapper) {
    if (!(value instanceof String text)) {
      return null;
    }
    try {
      return jsonMapper.readValue(text, SchedulerCommandEnvelope.class);
    } catch (JacksonException | IllegalArgumentException exception) {
      return null;
    }
  }

  private static String errorCode(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof KafkaCommandMapper.KafkaCommandException kafkaException) {
        return kafkaException.code();
      }
      if (current instanceof SchedulerCommandException commandException) {
        return commandException.code();
      }
      current = current.getCause();
    }
    return "KAFKA_COMMAND_FAILED";
  }

  private static void addHeader(RecordHeaders headers, String name, String value) {
    if (value != null && !value.isBlank()) {
      headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
