package io.k2iot.mcs.scheduler.grpc;

import io.k2iot.mcs.scheduler.command.SchedulerCommandAutoConfiguration;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.persistence.SchedulerJdbcPersistenceAutoConfiguration;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(
    after = {
      SchedulerJdbcPersistenceAutoConfiguration.class,
      SchedulerCommandAutoConfiguration.class
    })
public class SchedulerGrpcAutoConfiguration {

  @Bean
  @ConditionalOnBean(SchedulerCommandFacade.class)
  @ConditionalOnMissingBean(SchedulerCommandGrpcService.class)
  SchedulerCommandGrpcService schedulerCommandGrpcService(
      SchedulerCommandFacade facade, GrpcCommandMapper mapper, GrpcErrorMapper errorMapper) {
    return new SchedulerCommandGrpcService(facade, mapper, errorMapper);
  }

  @Bean
  @ConditionalOnBean({JobRepository.class, TriggerRepository.class})
  @ConditionalOnMissingBean(SchedulerQueryGrpcService.class)
  SchedulerQueryGrpcService schedulerQueryGrpcService(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      GrpcCommandMapper mapper,
      GrpcErrorMapper errorMapper) {
    return new SchedulerQueryGrpcService(jobRepository, triggerRepository, mapper, errorMapper);
  }
}
