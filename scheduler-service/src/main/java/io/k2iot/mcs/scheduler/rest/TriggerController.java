package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
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
@RequestMapping("/api/v1/triggers")
public final class TriggerController {

  private final SchedulerCommandFacade facade;
  private final RestCommandMapper mapper;

  public TriggerController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    this.facade = Objects.requireNonNull(facade, "facade");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @PostMapping
  public ResponseEntity<RestModels.TriggerResponse> createTrigger(
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestBody RestModels.CreateTriggerRequest request) {
    TriggerDefinition trigger =
        facade.createTrigger(mapper.createTrigger(mapper.requestId(idempotencyKey), request));
    return ResponseEntity.created(URI.create("/api/v1/triggers/" + trigger.triggerId()))
        .eTag(etag(trigger.revision()))
        .body(new RestModels.TriggerResponse(trigger));
  }

  @PutMapping("/{triggerId}")
  public ResponseEntity<RestModels.TriggerResponse> replaceTrigger(
      @PathVariable UUID triggerId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.ReplaceTriggerRequest request) {
    mapper.requireResourceId(
        triggerId, request.trigger() == null ? null : request.trigger().triggerId());
    TriggerDefinition trigger =
        facade.replaceTrigger(
            mapper.replaceTrigger(
                mapper.requestId(idempotencyKey), mapper.expectedRevision(ifMatch), request));
    return ResponseEntity.ok()
        .eTag(etag(trigger.revision()))
        .body(new RestModels.TriggerResponse(trigger));
  }

  @PostMapping("/{triggerId}/pause")
  public ResponseEntity<RestModels.TriggerResponse> pauseTrigger(
      @PathVariable UUID triggerId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.MutationRequest request) {
    TriggerDefinition trigger =
        facade.pauseTrigger(
            mapper.triggerMutation(
                mapper.requestId(idempotencyKey),
                triggerId,
                mapper.expectedRevision(ifMatch),
                request));
    return ResponseEntity.ok()
        .eTag(etag(trigger.revision()))
        .body(new RestModels.TriggerResponse(trigger));
  }

  @PostMapping("/{triggerId}/resume")
  public ResponseEntity<RestModels.TriggerResponse> resumeTrigger(
      @PathVariable UUID triggerId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.MutationRequest request) {
    TriggerDefinition trigger =
        facade.resumeTrigger(
            mapper.triggerMutation(
                mapper.requestId(idempotencyKey),
                triggerId,
                mapper.expectedRevision(ifMatch),
                request));
    return ResponseEntity.ok()
        .eTag(etag(trigger.revision()))
        .body(new RestModels.TriggerResponse(trigger));
  }

  @DeleteMapping("/{triggerId}")
  public ResponseEntity<Void> deleteTrigger(
      @PathVariable UUID triggerId,
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestHeader("If-Match") String ifMatch,
      @RequestBody RestModels.MutationRequest request) {
    facade.deleteTrigger(
        mapper.triggerMutation(
            mapper.requestId(idempotencyKey),
            triggerId,
            mapper.expectedRevision(ifMatch),
            request));
    return ResponseEntity.noContent().build();
  }

  private static String etag(long revision) {
    return "\"" + revision + "\"";
  }
}
