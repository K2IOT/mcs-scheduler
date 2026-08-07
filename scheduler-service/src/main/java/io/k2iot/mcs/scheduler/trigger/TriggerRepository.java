package io.k2iot.mcs.scheduler.trigger;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TriggerRepository {

  Optional<TriggerDefinition> findById(UUID triggerId);

  List<TriggerDefinition> findByJobId(UUID jobId);

  List<TriggerDefinition> findPausedByJobId(UUID jobId, PauseReason pauseReason);

  List<TriggerDefinition> findPage(
      String namespace, Instant createdAfter, UUID idAfter, int limit);

  List<TriggerDefinition> findByJobIdPage(
      UUID jobId, Instant createdAfter, UUID idAfter, int limit);

  Optional<PauseReason> findPauseReason(UUID triggerId);

  void insert(TriggerDefinition definition);

  boolean update(TriggerDefinition definition, long expectedRevision);

  void setPauseReason(UUID triggerId, PauseReason pauseReason);

  enum PauseReason {
    INDIVIDUAL,
    JOB
  }
}
