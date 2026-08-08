package io.k2iot.mcs.scheduler.command;

import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.execution.ExecutionEventFactory;
import io.k2iot.mcs.scheduler.execution.ExecutionRepository;
import io.k2iot.mcs.scheduler.execution.ScheduledExecutionService;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.outbox.OutboxRepository;
import io.k2iot.mcs.scheduler.persistence.SchedulerJdbcPersistenceAutoConfiguration;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(after = SchedulerJdbcPersistenceAutoConfiguration.class)
public class SchedulerCommandAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(Clock.class)
  Clock schedulerClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean(ExecutionEventFactory.class)
  ExecutionEventFactory executionEventFactory(JsonMapper jsonMapper) {
    return new ExecutionEventFactory(jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean(ScheduledExecutionService.class)
  @ConditionalOnBean({
    JobRepository.class,
    TriggerRepository.class,
    DestinationRepository.class,
    ExecutionRepository.class,
    OutboxRepository.class
  })
  ScheduledExecutionService scheduledExecutionService(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      ExecutionRepository executionRepository,
      OutboxRepository outboxRepository,
      ExecutionEventFactory eventFactory,
      Clock clock) {
    return new ScheduledExecutionService(
        jobRepository,
        triggerRepository,
        destinationRepository,
        executionRepository,
        outboxRepository,
        eventFactory,
        clock);
  }

  @Bean
  @ConditionalOnMissingBean(SchedulerCommandFacade.class)
  @ConditionalOnBean({
    JobRepository.class,
    TriggerRepository.class,
    DestinationRepository.class,
    CommandRequestRepository.class,
    SchedulerProjectionPort.class,
    AuditRepository.class
  })
  SchedulerCommandFacade schedulerCommandFacade(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      CommandRequestRepository commandRequestRepository,
      SchedulerProjectionPort schedulerProjection,
      JsonMapper jsonMapper,
      Clock clock,
      AuditRepository auditRepository) {
    return new LifecycleSchedulerCommandFacade(
        jobRepository,
        triggerRepository,
        destinationRepository,
        commandRequestRepository,
        schedulerProjection,
        jsonMapper,
        clock,
        auditRepository);
  }
}
