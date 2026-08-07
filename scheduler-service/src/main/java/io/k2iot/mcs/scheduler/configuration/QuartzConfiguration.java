package io.k2iot.mcs.scheduler.configuration;

import io.k2iot.mcs.scheduler.command.SchedulerProjectionPort;
import io.k2iot.mcs.scheduler.quartz.QuartzSchedulerProjection;
import io.k2iot.mcs.scheduler.quartz.QuartzTriggerMapper;
import org.quartz.Scheduler;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.quartz.autoconfigure.SchedulerFactoryBeanCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

@Configuration(proxyBeanMethods = false)
public class QuartzConfiguration {

  @Bean
  @ConditionalOnMissingBean(QuartzTriggerMapper.class)
  QuartzTriggerMapper quartzTriggerMapper() {
    return new QuartzTriggerMapper();
  }

  @Bean
  @ConditionalOnMissingBean(SchedulerProjectionPort.class)
  SchedulerProjectionPort quartzSchedulerProjection(
      ObjectProvider<Scheduler> schedulerProvider, QuartzTriggerMapper triggerMapper) {
    return new QuartzSchedulerProjection(schedulerProvider::getObject, triggerMapper);
  }

  @Bean
  @ConditionalOnMissingBean(name = "schedulerQuartzFactoryBeanCustomizer")
  SchedulerFactoryBeanCustomizer schedulerQuartzFactoryBeanCustomizer(
      ApplicationContext applicationContext) {
    AutowireCapableBeanFactory beanFactory = applicationContext.getAutowireCapableBeanFactory();
    SpringBeanJobFactory jobFactory =
        new SpringBeanJobFactory() {
          @Override
          protected Object createJobInstance(TriggerFiredBundle bundle) {
            return beanFactory.createBean(bundle.getJobDetail().getJobClass());
          }
        };
    jobFactory.setApplicationContext(applicationContext);
    return schedulerFactoryBean -> schedulerFactoryBean.setJobFactory(jobFactory);
  }
}
