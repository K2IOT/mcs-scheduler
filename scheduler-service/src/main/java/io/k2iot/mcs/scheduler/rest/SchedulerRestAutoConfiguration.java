package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.SchedulerCommandAutoConfiguration;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = SchedulerCommandAutoConfiguration.class)
@ConditionalOnBean({SchedulerCommandFacade.class, RestCommandMapper.class})
public class SchedulerRestAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  ScheduleController scheduleController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    return new ScheduleController(facade, mapper);
  }

  @Bean
  @ConditionalOnMissingBean
  JobController jobController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    return new JobController(facade, mapper);
  }

  @Bean
  @ConditionalOnMissingBean
  TriggerController triggerController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    return new TriggerController(facade, mapper);
  }

  @Bean
  @ConditionalOnMissingBean
  ExecutionController executionController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    return new ExecutionController(facade, mapper);
  }
}
