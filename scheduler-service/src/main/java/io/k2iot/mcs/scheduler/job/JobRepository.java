package io.k2iot.mcs.scheduler.job;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository {

  Optional<JobDefinition> findById(UUID jobId);

  List<JobDefinition> findPage(String namespace, Instant createdAfter, UUID idAfter, int limit);

  void insert(JobDefinition definition);

  boolean update(JobDefinition definition, long expectedRevision);
}
