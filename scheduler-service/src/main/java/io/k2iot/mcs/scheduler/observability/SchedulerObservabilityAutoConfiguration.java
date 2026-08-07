package io.k2iot.mcs.scheduler.observability;

import io.k2iot.mcs.scheduler.command.SchedulerProjectionPort;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.quartz.Scheduler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;

@AutoConfiguration
public class SchedulerObservabilityAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(SchedulerMetrics.class)
  @ConditionalOnBean(MeterRegistry.class)
  SchedulerMetrics schedulerMetrics(MeterRegistry meterRegistry) {
    return new SchedulerMetrics(meterRegistry);
  }

  @Bean
  @ConditionalOnMissingBean(QuartzReconciler.class)
  @ConditionalOnBean({
    Scheduler.class,
    JobRepository.class,
    TriggerRepository.class,
    SchedulerProjectionPort.class
  })
  QuartzReconciler quartzReconciler(
      Scheduler scheduler,
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      SchedulerProjectionPort projection,
      SchedulerMetrics metrics) {
    return new QuartzReconciler(scheduler, jobRepository, triggerRepository, projection, metrics);
  }

  @Bean(name = "schedulerHealthIndicator")
  @ConditionalOnMissingBean(name = "schedulerHealthIndicator")
  @ConditionalOnBean({Scheduler.class, JdbcClient.class})
  SchedulerHealthIndicator schedulerHealthIndicator(
      Scheduler scheduler, JdbcClient jdbcClient, SchedulerMetrics metrics) {
    return new SchedulerHealthIndicator(scheduler, jdbcClient, metrics);
  }
}
