package io.k2iot.mcs.scheduler.client;

import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.ExecutionResponse;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import java.util.UUID;

public interface SchedulerClient {

  ScheduleResponse createSchedule(CreateScheduleRequest request, UUID requestId);

  JobResponse pauseJob(UUID jobId, String namespace, long expectedRevision, UUID requestId);

  JobResponse resumeJob(UUID jobId, String namespace, long expectedRevision, UUID requestId);

  void deleteJob(
      UUID jobId, String namespace, long expectedRevision, boolean cascade, UUID requestId);

  ExecutionResponse fireTrigger(
      UUID triggerId, String namespace, UUID manualFireId, UUID requestId);
}
