package io.k2iot.mcs.scheduler.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.GrpcChannelFactory;

class SchedulerClientAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(SchedulerClientAutoConfiguration.class))
          .withUserConfiguration(GrpcChannelFactoryConfiguration.class);

  @Test
  void createsGrpcClientWhenExplicitlyEnabled() {
    contextRunner
        .withPropertyValues(
            "mcs.scheduler.client.enabled=true",
            "mcs.scheduler.client.transport=grpc",
            "mcs.scheduler.client.grpc-target=localhost:9090")
        .run(
            context -> {
              assertThat(context).hasSingleBean(SchedulerClient.class);
              assertThat(context).doesNotHaveBean(AsyncSchedulerClient.class);
            });
  }

  @Test
  void staysDisabledByDefault() {
    contextRunner.run(context -> assertThat(context).doesNotHaveBean(SchedulerClient.class));
  }

  @Configuration(proxyBeanMethods = false)
  static class GrpcChannelFactoryConfiguration {

    @Bean
    GrpcChannelFactory grpcChannelFactory() {
      return mock(GrpcChannelFactory.class);
    }
  }
}
