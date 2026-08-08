package io.k2iot.mcs.scheduler.client;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration
@EnableConfigurationProperties(SchedulerClientProperties.class)
@ConditionalOnProperty(prefix = "mcs.scheduler.client", name = "enabled", havingValue = "true")
public class SchedulerClientAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(SchedulerClient.class)
  @ConditionalOnProperty(prefix = "mcs.scheduler.client", name = "transport", havingValue = "grpc")
  SchedulerClient grpcSchedulerClient(
      GrpcChannelFactory channels, SchedulerClientProperties properties) {
    return new GrpcSchedulerClient(
        channels,
        requireText(properties.getGrpcTarget(), "mcs.scheduler.client.grpc-target"),
        requireText(properties.getProducer(), "mcs.scheduler.client.producer"));
  }

  @Bean
  @ConditionalOnMissingBean(AsyncSchedulerClient.class)
  @ConditionalOnProperty(prefix = "mcs.scheduler.client", name = "transport", havingValue = "kafka")
  AsyncSchedulerClient kafkaSchedulerClient(
      KafkaTemplate<String, String> kafkaTemplate, SchedulerClientProperties properties) {
    return new KafkaSchedulerCommandPublisher(
        kafkaTemplate,
        requireText(properties.getKafkaCommandTopic(), "mcs.scheduler.client.kafka-command-topic"),
        requireText(properties.getProducer(), "mcs.scheduler.client.producer"));
  }

  private static String requireText(String value, String property) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(property + " must be configured");
    }
    return value;
  }
}
