package io.k2iot.mcs.scheduler.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.BindableService;
import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.grpc.server.lifecycle.GrpcServerLifecycle;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = "spring.grpc.server.port=0")
class GrpcAutoConfigurationIT extends PostgresIntegrationTestBase {

  @Autowired ApplicationContext applicationContext;

  @Test
  void registersBindableSchedulerServiceAndStartsGrpcServer() {
    Map<String, BindableService> services =
        applicationContext.getBeansOfType(BindableService.class);
    Map<String, GrpcServerLifecycle> lifecycles =
        applicationContext.getBeansOfType(GrpcServerLifecycle.class);

    assertThat(services)
        .as("scheduler gRPC BindableService beans")
        .containsKey("schedulerQueryGrpcService");
    assertThat(lifecycles).as("network gRPC server lifecycle").hasSize(1);

    GrpcServerLifecycle lifecycle = lifecycles.values().iterator().next();
    assertThat(lifecycle.isRunning()).isTrue();
    assertThat(lifecycle.getPort()).isPositive();
  }
}
