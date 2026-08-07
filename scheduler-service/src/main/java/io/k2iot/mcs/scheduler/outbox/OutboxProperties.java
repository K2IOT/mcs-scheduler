package io.k2iot.mcs.scheduler.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mcs.scheduler.outbox")
public class OutboxProperties {

  private boolean enabled = true;
  private int batchSize = 100;
  private Duration pollInterval = Duration.ofSeconds(1);
  private Duration claimTimeout = Duration.ofSeconds(30);
  private Duration publishTimeout = Duration.ofSeconds(30);
  private int maxAttempts = 20;
  private Duration maxAge = Duration.ofHours(24);

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public Duration getPollInterval() {
    return pollInterval;
  }

  public void setPollInterval(Duration pollInterval) {
    this.pollInterval = pollInterval;
  }

  public Duration getClaimTimeout() {
    return claimTimeout;
  }

  public void setClaimTimeout(Duration claimTimeout) {
    this.claimTimeout = claimTimeout;
  }

  public Duration getPublishTimeout() {
    return publishTimeout;
  }

  public void setPublishTimeout(Duration publishTimeout) {
    this.publishTimeout = publishTimeout;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  public Duration getMaxAge() {
    return maxAge;
  }

  public void setMaxAge(Duration maxAge) {
    this.maxAge = maxAge;
  }
}
