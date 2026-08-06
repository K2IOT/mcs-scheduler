package io.k2iot.mcs.scheduler.command;

import java.util.Objects;
import java.util.UUID;

public record ManualFireResult(UUID manualFireId, UUID triggerId, UUID jobId) {

  public ManualFireResult {
    Objects.requireNonNull(manualFireId, "manualFireId");
    Objects.requireNonNull(triggerId, "triggerId");
    Objects.requireNonNull(jobId, "jobId");
  }
}
