package io.k2iot.mcs.scheduler.persistence;

import io.k2iot.mcs.scheduler.command.CommandRequestRepository;
import io.k2iot.mcs.scheduler.command.JdbcCommandRequestRepository;
import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.destination.JdbcDestinationRepository;
import io.k2iot.mcs.scheduler.execution.ExecutionRepository;
import io.k2iot.mcs.scheduler.execution.JdbcExecutionRepository;
import io.k2iot.mcs.scheduler.job.JdbcJobRepository;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.outbox.JdbcOutboxClaimRepository;
import io.k2iot.mcs.scheduler.outbox.JdbcOutboxRepository;
import io.k2iot.mcs.scheduler.outbox.OutboxClaimRepository;
import io.k2iot.mcs.scheduler.outbox.OutboxRepository;
import io.k2iot.mcs.scheduler.trigger.JdbcTriggerRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.jdbc.autoconfigure.JdbcClientAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(after = JdbcClientAutoConfiguration.class)
@ConditionalOnClass({JdbcClient.class, JsonMapper.class})
@ConditionalOnBean(JdbcClient.class)
public class SchedulerJdbcPersistenceAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(JobRepository.class)
  JobRepository jobRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcJobRepository(jdbcClient, jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(TriggerRepository.class)
  TriggerRepository triggerRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcTriggerRepository(jdbcClient, jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(DestinationRepository.class)
  DestinationRepository destinationRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcDestinationRepository(jdbcClient, jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(CommandRequestRepository.class)
  CommandRequestRepository commandRequestRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcCommandRequestRepository(jdbcClient, jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(ExecutionRepository.class)
  ExecutionRepository executionRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcExecutionRepository(jdbcClient, jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(OutboxRepository.class)
  OutboxRepository outboxRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcOutboxRepository(jdbcClient, jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(OutboxClaimRepository.class)
  OutboxClaimRepository outboxClaimRepository(JdbcClient jdbcClient, JsonMapper jsonMapper) {
    return new JdbcOutboxClaimRepository(jdbcClient, jsonMapper);
  }
}
