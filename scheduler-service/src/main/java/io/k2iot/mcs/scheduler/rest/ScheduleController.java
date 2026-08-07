package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/schedules")
public final class ScheduleController {

  private final SchedulerCommandFacade facade;
  private final RestCommandMapper mapper;

  public ScheduleController(SchedulerCommandFacade facade, RestCommandMapper mapper) {
    this.facade = Objects.requireNonNull(facade, "facade");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @PostMapping
  public ResponseEntity<RestModels.ScheduleResponse> createSchedule(
      @RequestHeader("Idempotency-Key") String idempotencyKey,
      @RequestBody RestModels.CreateScheduleRequest request) {
    UUID requestId = mapper.requestId(idempotencyKey);
    SchedulerCommands.ScheduleResult result =
        facade.createSchedule(mapper.createSchedule(requestId, request));
    return ResponseEntity.created(URI.create("/api/v1/jobs/" + result.job().jobId()))
        .eTag(etag(result.job().revision()))
        .body(new RestModels.ScheduleResponse(result.job(), result.triggers()));
  }

  private static String etag(long revision) {
    return "\"" + revision + "\"";
  }
}
