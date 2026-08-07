package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.ManualFireResult;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import java.net.URI;
import java.util.Objects;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnBean(SchedulerCommandFacade.class)
@RequestMapping("/api/v1/executions")
public final class ExecutionController {

  private final SchedulerCommandFacade facade;
  private final RestCommandMapper mapper;

  public ExecutionController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    this.facade = Objects.requireNonNull(facade, "facade");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @PostMapping
  public ResponseEntity<RestModels.ExecutionResponse> fireTriggerNow(
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestBody RestModels.FireTriggerRequest request) {
    ManualFireResult result =
        facade.fireTriggerNow(mapper.fireTriggerNow(mapper.requestId(idempotencyKey), request));
    return ResponseEntity.created(URI.create("/api/v1/executions/" + result.manualFireId()))
        .body(
            new RestModels.ExecutionResponse(
                result.manualFireId(), result.triggerId(), result.jobId()));
  }
}
