package io.k2iot.mcs.scheduler.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.v1.CreateJobRequest;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.CreateTriggerRequest;
import io.k2iot.mcs.scheduler.v1.DeleteJobRequest;
import io.k2iot.mcs.scheduler.v1.DeleteTriggerRequest;
import io.k2iot.mcs.scheduler.v1.ExecutionResponse;
import io.k2iot.mcs.scheduler.v1.FireTriggerRequest;
import io.k2iot.mcs.scheduler.v1.JobMutationRequest;
import io.k2iot.mcs.scheduler.v1.JobResponse;
import io.k2iot.mcs.scheduler.v1.ReplaceTriggerRequest;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.SchedulerCommandServiceGrpc;
import io.k2iot.mcs.scheduler.v1.TriggerMutationRequest;
import io.k2iot.mcs.scheduler.v1.TriggerResponse;
import io.k2iot.mcs.scheduler.v1.UpdateJobRequest;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean(SchedulerCommandFacade.class)
public final class SchedulerCommandGrpcService
    extends SchedulerCommandServiceGrpc.SchedulerCommandServiceImplBase {

  private final SchedulerCommandFacade facade;
  private final GrpcCommandMapper mapper;
  private final GrpcErrorMapper errorMapper;

  public SchedulerCommandGrpcService(
      SchedulerCommandFacade facade, GrpcCommandMapper mapper, GrpcErrorMapper errorMapper) {
    this.facade = Objects.requireNonNull(facade, "facade");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
  }

  @Override
  public void createJob(CreateJobRequest request, StreamObserver<JobResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.jobResponse(facade.createJob(mapper.createJob(request))));
  }

  @Override
  public void updateJob(UpdateJobRequest request, StreamObserver<JobResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.jobResponse(facade.updateJob(mapper.updateJob(request))));
  }

  @Override
  public void pauseJob(JobMutationRequest request, StreamObserver<JobResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.jobResponse(facade.pauseJob(mapper.jobMutation(request))));
  }

  @Override
  public void resumeJob(JobMutationRequest request, StreamObserver<JobResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.jobResponse(facade.resumeJob(mapper.jobMutation(request))));
  }

  @Override
  public void deleteJob(DeleteJobRequest request, StreamObserver<Empty> responseObserver) {
    respond(
        responseObserver,
        () -> {
          facade.deleteJob(mapper.deleteJob(request));
          return Empty.getDefaultInstance();
        });
  }

  @Override
  public void createTrigger(
      CreateTriggerRequest request, StreamObserver<TriggerResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.triggerResponse(facade.createTrigger(mapper.createTrigger(request))));
  }

  @Override
  public void replaceTrigger(
      ReplaceTriggerRequest request, StreamObserver<TriggerResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.triggerResponse(facade.replaceTrigger(mapper.replaceTrigger(request))));
  }

  @Override
  public void pauseTrigger(
      TriggerMutationRequest request, StreamObserver<TriggerResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.triggerResponse(facade.pauseTrigger(mapper.triggerMutation(request))));
  }

  @Override
  public void resumeTrigger(
      TriggerMutationRequest request, StreamObserver<TriggerResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.triggerResponse(facade.resumeTrigger(mapper.triggerMutation(request))));
  }

  @Override
  public void deleteTrigger(DeleteTriggerRequest request, StreamObserver<Empty> responseObserver) {
    respond(
        responseObserver,
        () -> {
          facade.deleteTrigger(mapper.deleteTrigger(request));
          return Empty.getDefaultInstance();
        });
  }

  @Override
  public void fireTriggerNow(
      FireTriggerRequest request, StreamObserver<ExecutionResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.executionResponse(facade.fireTriggerNow(mapper.fireTriggerNow(request))));
  }

  @Override
  public void createSchedule(
      CreateScheduleRequest request, StreamObserver<ScheduleResponse> responseObserver) {
    respond(
        responseObserver,
        () -> mapper.scheduleResponse(facade.createSchedule(mapper.createSchedule(request))));
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
