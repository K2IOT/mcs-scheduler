package io.k2iot.mcs.scheduler.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mcs.scheduler.client")
public class SchedulerClientProperties {

  public enum Transport {
    GRPC,
    KAFKA
  }

  private boolean enabled;
  private Transport transport = Transport.GRPC;
  private String grpcTarget;
  private String kafkaCommandTopic;
  private String producer = "mcs-scheduler-client";

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Transport getTransport() {
    return transport;
  }

  public void setTransport(Transport transport) {
    this.transport = transport;
  }

  public String getGrpcTarget() {
    return grpcTarget;
  }

  public void setGrpcTarget(String grpcTarget) {
    this.grpcTarget = grpcTarget;
  }

  public String getKafkaCommandTopic() {
    return kafkaCommandTopic;
  }

  public void setKafkaCommandTopic(String kafkaCommandTopic) {
    this.kafkaCommandTopic = kafkaCommandTopic;
  }

  public String getProducer() {
    return producer;
  }

  public void setProducer(String producer) {
    this.producer = producer;
  }
}
