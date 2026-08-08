package io.k2iot.mcs.scheduler.client;

import java.util.Objects;
import java.util.UUID;

public record CommandReceipt(UUID requestId, UUID messageId, String topic) {

  public CommandReceipt {
    Objects.requireNonNull(requestId, "requestId");
    Objects.requireNonNull(messageId, "messageId");
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
  }
}
