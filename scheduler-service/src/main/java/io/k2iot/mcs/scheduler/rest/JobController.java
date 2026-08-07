package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/jobs")
public final class JobController {

  private final SchedulerCommandFacade facade;
  private final RestCommandMapper mapper;

  public JobController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    this.facade = Objects.requireNonNull(facade, "facade");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @PostMapping
  public ResponseEntity<RestModels.JobResponse> createJob(
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestBody RestModels.CreateJobRequest request) {
    JobDefinition job =
        facade.createJob(mapper.createJob(mapper.requestId(idempotencyKey), request));
    return ResponseEntity.created(URI.create("/api/v1/jobs/" + job.jobId()))
        .eTag(etag(job.revision()))
        .body(new RestModels.JobResponse(job));
  }

  @PutMapping("/{jobId}")
  public ResponseEntity<RestModels.JobResponse> updateJob(
      @PathVariable UUID jobId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.UpdateJobRequest request) {
    mapper.requireResourceId(jobId, request.job() == null ? null : request.job().jobId());
    JobDefinition job =
        facade.updateJob(
            mapper.updateJob(
                mapper.requestId(idempotencyKey), mapper.expectedRevision(ifMatch), request));
    return ResponseEntity.ok().eTag(etag(job.revision())).body(new RestModels.JobResponse(job));
  }

  @PostMapping("/{jobId}/pause")
  public ResponseEntity<RestModels.JobResponse> pauseJob(
      @PathVariable UUID jobId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.MutationRequest request) {
    JobDefinition job =
        facade.pauseJob(
            mapper.jobMutation(
                mapper.requestId(idempotencyKey),
                jobId,
                mapper.expectedRevision(ifMatch),
                request));
    return ResponseEntity.ok().eTag(etag(job.revision())).body(new RestModels.JobResponse(job));
  }

  @PostMapping("/{jobId}/resume")
  public ResponseEntity<RestModels.JobResponse> resumeJob(
      @PathVariable UUID jobId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.MutationRequest request) {
    JobDefinition job =
        facade.resumeJob(
            mapper.jobMutation(
                mapper.requestId(idempotencyKey),
                jobId,
                mapper.expectedRevision(ifMatch),
                request));
    return ResponseEntity.ok().eTag(etag(job.revision())).body(new RestModels.JobResponse(job));
  }

  @DeleteMapping("/{jobId}")
  public ResponseEntity<Void> deleteJob(
      @PathVariable UUID jobId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.MutationRequest request) {
    facade.deleteJob(
        mapper.jobMutation(
            mapper.requestId(idempotencyKey), jobId, mapper.expectedRevision(ifMatch), request));
    return ResponseEntity.noContent().build();
  }

  private static String etag(long revision) {
    return "\"" + revision + "\"";
  }
}
