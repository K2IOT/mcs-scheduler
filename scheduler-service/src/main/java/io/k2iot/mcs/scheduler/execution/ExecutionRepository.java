package io.k2iot.mcs.scheduler.execution;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface ExecutionRepository {

  boolean insertIfAbsent(ExecutionRecord execution);

  Optional<ExecutionRecord> findById(UUID executionId);

  List<ExecutionRecord> findPage(
      String namespace, Instant scheduledFireAfter, UUID idAfter, int limit);

  enum Status {
    SCHEDULED,
    SUPPRESSED,
    DELIVERED,
    DELIVERY_FAILED
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
