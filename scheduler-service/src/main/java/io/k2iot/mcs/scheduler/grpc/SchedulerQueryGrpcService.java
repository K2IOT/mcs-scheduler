package io.k2iot.mcs.scheduler.grpc;

import io.grpc.stub.StreamObserver;
import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import io.k2iot.mcs.scheduler.v1.GetJobRequest;
import io.k2iot.mcs.scheduler.v1.GetTriggerRequest;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ListJobTriggersRequest;
import io.k2iot.mcs.scheduler.v1.ListTriggersResponse;
import io.k2iot.mcs.scheduler.v1.SchedulerQueryServiceGrpc;
import io.k2iot.mcs.scheduler.v1.TriggerResponse;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public final class SchedulerQueryGrpcService
    extends SchedulerQueryServiceGrpc.SchedulerQueryServiceImplBase {

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = 1000;

  private final JobRepository jobRepository;
  private final TriggerRepository triggerRepository;
  private final GrpcCommandMapper mapper;
  private final GrpcErrorMapper errorMapper;

  public SchedulerQueryGrpcService(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      GrpcCommandMapper mapper,
      GrpcErrorMapper errorMapper) {
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
    this.triggerRepository = Objects.requireNonNull(triggerRepository, "triggerRepository");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
  }

  @Override
  public void getJob(GetJobRequest request, StreamObserver<JobResponse> responseObserver) {
    respond(
        responseObserver,
        () -> {
          UUID jobId = mapper.uuid(request.getJobId(), "job_id");
          JobDefinition job = requireJob(jobId, request.getNamespace());
          return mapper.jobResponse(job);
        });
  }

  @Override
  public void listJobTriggers(
      ListJobTriggersRequest request, StreamObserver<ListTriggersResponse> responseObserver) {
    respond(
        responseObserver,
        () -> {
          UUID jobId = mapper.uuid(request.getJobId(), "job_id");
          requireJob(jobId, request.getNamespace());
          List<TriggerDefinition> matching =
              triggerRepository.findByJobId(jobId).stream()
                  .filter(trigger -> trigger.namespace().equals(request.getNamespace()))
                  .toList();
          int offset = pageOffset(request.getPageToken());
          int pageSize = pageSize(request.getPageSize());
          if (offset > matching.size()) {
            throw new IllegalArgumentException("page_token is outside the result set");
          }
          int end = Math.min(offset + pageSize, matching.size());
          ListTriggersResponse.Builder response = ListTriggersResponse.newBuilder();
          matching.subList(offset, end).stream()
              .map(mapper::triggerDefinition)
              .forEach(response::addTriggers);
          if (end < matching.size()) {
            response.setNextPageToken(Integer.toString(end));
          }
          return response.build();
        });
  }

  @Override
  public void getTrigger(
      GetTriggerRequest request, StreamObserver<TriggerResponse> responseObserver) {
    respond(
        responseObserver,
        () -> {
          UUID triggerId = mapper.uuid(request.getTriggerId(), "trigger_id");
          TriggerDefinition trigger = requireTrigger(triggerId, request.getNamespace());
          return mapper.triggerResponse(trigger);
        });
  }

  private JobDefinition requireJob(UUID jobId, String namespace) {
    JobDefinition job =
        jobRepository
            .findById(jobId)
            .orElseThrow(() -> new SchedulerCommandException("JOB_NOT_FOUND", "Job was not found"));
    if (!job.namespace().equals(requireNamespace(namespace))) {
      throw new SchedulerCommandException("JOB_NOT_FOUND", "Job was not found");
    }
    return job;
  }

  private TriggerDefinition requireTrigger(UUID triggerId, String namespace) {
    TriggerDefinition trigger =
        triggerRepository
            .findById(triggerId)
            .orElseThrow(
                () -> new SchedulerCommandException("TRIGGER_NOT_FOUND", "Trigger was not found"));
    if (!trigger.namespace().equals(requireNamespace(namespace))) {
      throw new SchedulerCommandException("TRIGGER_NOT_FOUND", "Trigger was not found");
    }
    return trigger;
  }

  private static String requireNamespace(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be blank");
    }
    return namespace;
  }

  private static int pageSize(int requested) {
    if (requested < 0) {
      throw new IllegalArgumentException("page_size must not be negative");
    }
    if (requested == 0) {
      return DEFAULT_PAGE_SIZE;
    }
    return Math.min(requested, MAX_PAGE_SIZE);
  }

  private static int pageOffset(String token) {
    if (token == null || token.isBlank()) {
      return 0;
    }
    try {
      int offset = Integer.parseInt(token);
      if (offset < 0) {
        throw new NumberFormatException("negative offset");
      }
      return offset;
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException("page_token is invalid", exception);
    }
  }

  private <T> void respond(StreamObserver<T> responseObserver, Supplier<T> action) {
    try {
      responseObserver.onNext(action.get());
      responseObserver.onCompleted();
    } catch (RuntimeException exception) {
      responseObserver.onError(errorMapper.toStatusRuntimeException(exception));
    }
  }
}
