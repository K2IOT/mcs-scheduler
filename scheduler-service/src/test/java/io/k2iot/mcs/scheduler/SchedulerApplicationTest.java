package io.k2iot.mcs.scheduler;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    properties = {
      "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,org.springframework.boot.quartz.autoconfigure.QuartzAutoConfiguration,org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"
    })
class SchedulerApplicationTest {

  @Test
  void contextLoads() {}
}
