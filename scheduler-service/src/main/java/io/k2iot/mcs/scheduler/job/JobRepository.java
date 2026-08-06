package io.k2iot.mcs.scheduler.job;

import java.util.Optional;
import java.util.UUID;

public interface JobRepository {

  Optional<JobDefinition> findById(UUID jobId);

  void insert(JobDefinition definition);

  boolean update(JobDefinition definition, long expectedRevision);
}
