package io.k2iot.mcs.scheduler.trigger;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TriggerRepository {

  Optional<TriggerDefinition> findById(UUID triggerId);

  List<TriggerDefinition> findByJobId(UUID jobId);

  void insert(TriggerDefinition definition);

  boolean update(TriggerDefinition definition, long expectedRevision);
}
