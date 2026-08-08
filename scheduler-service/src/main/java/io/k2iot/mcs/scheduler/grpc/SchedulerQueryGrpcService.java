package io.k2iot.mcs.scheduler.grpc;

import io.grpc.stub.StreamObserver;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobQueryService;
import io.k2iot.mcs.scheduler.job.JobRepository;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerQueryService;
import io.k2iot.mcs.scheduler.trigger.TriggerRepository;
import io.k2iot.mcs.scheduler.v1.GetJobRequest;
import io.k2iot.mcs.scheduler.v1.GetTriggerRequest;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ListJobTriggersRequest;
import io.k2iot.mcs.scheduler.v1.ListTriggersResponse;
import io.k2iot.mcs.scheduler.v1.SchedulerQueryServiceGrpc;
import io.k2iot.mcs.scheduler.v1.TriggerResponse;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public final class SchedulerQueryGrpcService
    extends SchedulerQueryServiceGrpc.SchedulerQueryServiceImplBase {

  private final JobQueryService jobQueryService;
  private final TriggerQueryService triggerQueryService;
  private final GrpcCommandMapper mapper;
  private final GrpcErrorMapper errorMapper;

  public SchedulerQueryGrpcService(
      JobRepository jobRepository,
      TriggerRepository triggerRepository,
      GrpcCommandMapper mapper,
      GrpcErrorMapper errorMapper) {
    this(
        new JobQueryService(jobRepository),
        new TriggerQueryService(triggerRepository, jobRepository),
        mapper,
        errorMapper);
  }

  public SchedulerQueryGrpcService(
      JobQueryService jobQueryService,
      TriggerQueryService triggerQueryService,
      GrpcCommandMapper mapper,
      GrpcErrorMapper errorMapper) {
    this.jobQueryService = Objects.requireNonNull(jobQueryService, "jobQueryService");
    this.triggerQueryService = Objects.requireNonNull(triggerQueryService, "triggerQueryService");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
  }

  @Override
  public void getJob(GetJobRequest request, StreamObserver<JobResponse> responseObserver) {
    respond(
        responseObserver,
        () -> {
          UUID jobId = mapper.uuid(request.getJobId(), "job_id");
          JobDefinition job = jobQueryService.get(jobId, request.getNamespace());
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
          TriggerQueryService.Page page =
              triggerQueryService.listByJob(
                  jobId, request.getNamespace(), request.getPageSize(), request.getPageToken());
          ListTriggersResponse.Builder response = ListTriggersResponse.newBuilder();
          page.items().stream().map(mapper::triggerDefinition).forEach(response::addTriggers);
          if (page.nextPageToken() != null) {
            response.setNextPageToken(page.nextPageToken());
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
          TriggerDefinition trigger = triggerQueryService.get(triggerId, request.getNamespace());
          return mapper.triggerResponse(trigger);
        });
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
