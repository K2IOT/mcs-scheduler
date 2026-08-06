package io.k2iot.mcs.scheduler.command;

import io.k2iot.mcs.scheduler.destination.DestinationRepository;
import io.k2iot.mcs.scheduler.job.JobRepository;
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
  @ConditionalOnMissingBean(SchedulerCommandFacade.class)
  @ConditionalOnBean({
    JobRepository.class,
    TriggerRepository.class,
    DestinationRepository.class,
    CommandRequestRepository.class,
    SchedulerProjectionPort.class
  })
  SchedulerCommandFacade schedulerCommandFacade(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      DestinationRepository destinationRepository,
      CommandRequestRepository commandRequestRepository,
      SchedulerProjectionPort schedulerProjection,
      JsonMapper jsonMapper,
      Clock clock) {
    return new SchedulerCommandFacade(
        jobRepository,
        triggerRepository,
        destinationRepository,
        commandRequestRepository,
        schedulerProjection,
        jsonMapper,
        clock);
  }
}
