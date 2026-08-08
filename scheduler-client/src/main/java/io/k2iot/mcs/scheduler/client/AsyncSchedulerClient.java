package io.k2iot.mcs.scheduler.client;

import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import java.util.UUID;

public interface AsyncSchedulerClient {

  CommandReceipt createSchedule(CreateScheduleRequest request, UUID requestId);

  CommandReceipt pauseJob(UUID jobId, String namespace, long expectedRevision, UUID requestId);

  CommandReceipt resumeJob(UUID jobId, String namespace, long expectedRevision, UUID requestId);

  CommandReceipt deleteJob(
      UUID jobId, String namespace, long expectedRevision, boolean cascade, UUID requestId);

  CommandReceipt fireTrigger(UUID triggerId, String namespace, UUID manualFireId, UUID requestId);
}
