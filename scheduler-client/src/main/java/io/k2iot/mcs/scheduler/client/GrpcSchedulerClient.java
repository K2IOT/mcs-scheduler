package io.k2iot.mcs.scheduler.client;

import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.DeleteJobRequest;
import io.k2iot.mcs.scheduler.v1.ExecutionResponse;
import io.k2iot.mcs.scheduler.v1.FireTriggerRequest;
import io.k2iot.mcs.scheduler.v1.JobMutationRequest;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.SchedulerCommandServiceGrpc;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.grpc.client.GrpcChannelFactory;

public final class GrpcSchedulerClient implements SchedulerClient {

  private final Supplier<SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub> stubs;
  private final Supplier<UUID> requestIds;
  private final String caller;
  private volatile SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub stub;

  public GrpcSchedulerClient(
      SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub stub,
      Supplier<UUID> requestIds) {
    this(() -> Objects.requireNonNull(stub, "stub"), requestIds, "mcs-scheduler-client");
    this.stub = stub;
  }

  public GrpcSchedulerClient(GrpcChannelFactory channels, String target, String caller) {
    this(
        () -> SchedulerCommandServiceGrpc.newBlockingStub(channels.createChannel(requireText(target, "target"))),
        UUID::randomUUID,
        caller);
  }

  GrpcSchedulerClient(
      Supplier<SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub> stubs,
      Supplier<UUID> requestIds,
      String caller) {
    this.stubs = Objects.requireNonNull(stubs, "stubs");
    this.requestIds = Objects.requireNonNull(requestIds, "requestIds");
    this.caller = requireText(caller, "caller");
  }

  @Override
  public ScheduleResponse createSchedule(CreateScheduleRequest request, UUID requestId) {
    Objects.requireNonNull(request, "request");
    UUID effectiveRequestId = requestId(requestId);
    CreateScheduleRequest wireRequest =
        request.toBuilder().setRequestId(effectiveRequestId.toString()).build();
    return stub().createSchedule(wireRequest);
  }

  @Override
  public JobResponse pauseJob(UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    return stub().pauseJob(jobMutation(jobId, namespace, expectedRevision, requestId));
  }

  @Override
  public JobResponse resumeJob(UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    return stub().resumeJob(jobMutation(jobId, namespace, expectedRevision, requestId));
  }

  @Override
  public void deleteJob(
      UUID jobId, String namespace, long expectedRevision, boolean cascade, UUID requestId) {
    UUID effectiveRequestId = requestId(requestId);
    DeleteJobRequest request =
        DeleteJobRequest.newBuilder()
            .setRequestId(effectiveRequestId.toString())
            .setNamespace(requireText(namespace, "namespace"))
            .setCaller(caller)
            .setJobId(Objects.requireNonNull(jobId, "jobId").toString())
            .setExpectedRevision(positiveRevision(expectedRevision))
            .setCascade(cascade)
            .build();
    stub().deleteJob(request);
  }

  @Override
  public ExecutionResponse fireTrigger(
      UUID triggerId, String namespace, UUID manualFireId, UUID requestId) {
    UUID effectiveRequestId = requestId(requestId);
    FireTriggerRequest request =
        FireTriggerRequest.newBuilder()
            .setRequestId(effectiveRequestId.toString())
            .setNamespace(requireText(namespace, "namespace"))
            .setCaller(caller)
            .setTriggerId(Objects.requireNonNull(triggerId, "triggerId").toString())
            .setManualFireId(Objects.requireNonNull(manualFireId, "manualFireId").toString())
            .build();
    return stub().fireTriggerNow(request);
  }

  private JobMutationRequest jobMutation(
      UUID jobId, String namespace, long expectedRevision, UUID requestId) {
    UUID effectiveRequestId = requestId(requestId);
    return JobMutationRequest.newBuilder()
        .setRequestId(effectiveRequestId.toString())
        .setNamespace(requireText(namespace, "namespace"))
        .setCaller(caller)
        .setJobId(Objects.requireNonNull(jobId, "jobId").toString())
        .setExpectedRevision(positiveRevision(expectedRevision))
        .build();
  }

  private SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub stub() {
    SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub current = stub;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (stub == null) {
        stub = Objects.requireNonNull(stubs.get(), "stub supplier returned null");
      }
      return stub;
    }
  }

  private UUID requestId(UUID supplied) {
    return supplied != null ? supplied : Objects.requireNonNull(requestIds.get(), "requestId");
  }

  private static long positiveRevision(long revision) {
    if (revision < 1) {
      throw new IllegalArgumentException("expectedRevision must be positive");
    }
    return revision;
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
