package io.k2iot.mcs.scheduler.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.BindableService;
import io.k2iot.mcs.scheduler.SchedulerApplication;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest(
    classes = SchedulerApplication.class,
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
      "spring.flyway.enabled=true",
      "spring.grpc.server.port=-1"
    })
class SchedulerGrpcRegistrationIT {

  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("scheduler")
          .withUsername("scheduler")
          .withPassword("scheduler");

  static {
    POSTGRES.start();
  }

  @DynamicPropertySource
  static void registerPostgresProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
  }

  @Autowired ApplicationContext applicationContext;

  @Test
  void productionContextRegistersCommandAndQueryGrpcServices() {
    List<Class<?>> grpcServiceTypes =
        applicationContext.getBeansOfType(BindableService.class).values().stream()
            .map(Object::getClass)
            .toList();

    assertThat(grpcServiceTypes)
        .anyMatch(SchedulerCommandGrpcService.class::isAssignableFrom)
        .anyMatch(SchedulerQueryGrpcService.class::isAssignableFrom);
  }
}
