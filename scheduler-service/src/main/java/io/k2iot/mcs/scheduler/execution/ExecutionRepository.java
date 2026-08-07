package io.k2iot.mcs.scheduler.execution;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public interface ExecutionRepository {

  boolean insertIfAbsent(ExecutionRecord execution);

  enum Status {
    SCHEDULED,
    SUPPRESSED
  }

  record ExecutionRecord(
      UUID executionId,
      UUID jobId,
      UUID triggerId,
      UUID manualFireId,
      Instant scheduledFireTime,
      Instant actualFireTime,
      Status status,
      int attempt,
      Map<String, Object> payload,
      Instant createdAt,
      Instant updatedAt) {}
}
