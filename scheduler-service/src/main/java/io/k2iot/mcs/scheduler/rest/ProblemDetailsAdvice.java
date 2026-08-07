package io.k2iot.mcs.scheduler.rest;

import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.trigger.InvalidTriggerException;
import java.net.URI;
import java.util.Locale;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public final class ProblemDetailsAdvice {

  @ExceptionHandler(SchedulerCommandException.class)
  ResponseEntity<ProblemDetail> handleSchedulerCommand(SchedulerCommandException exception) {
    return problem(statusFor(exception.code()), exception.code(), exception.getMessage());
  }

  @ExceptionHandler(InvalidTriggerException.class)
  ResponseEntity<ProblemDetail> handleInvalidTrigger(InvalidTriggerException exception) {
    return problem(HttpStatus.BAD_REQUEST, exception.code(), exception.getMessage());
  }

  @ExceptionHandler(RestCommandMapper.RestContractException.class)
  ResponseEntity<ProblemDetail> handleRestContract(
      RestCommandMapper.RestContractException exception) {
    return problem(HttpStatus.BAD_REQUEST, exception.code(), exception.getMessage());
  }

  @ExceptionHandler(MissingRequestHeaderException.class)
  ResponseEntity<ProblemDetail> handleMissingHeader(MissingRequestHeaderException exception) {
    String code =
        "Idempotency-Key".equalsIgnoreCase(exception.getHeaderName())
            ? "MISSING_IDEMPOTENCY_KEY"
            : "MISSING_REQUIRED_HEADER";
    return problem(
        HttpStatus.BAD_REQUEST,
        code,
        "Required request header is missing: " + exception.getHeaderName());
  }

  @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentNotValidException.class})
  ResponseEntity<ProblemDetail> handleMalformedRequest(Exception exception) {
    return problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Request body is invalid");
  }

  @ExceptionHandler(IllegalArgumentException.class)
  ResponseEntity<ProblemDetail> handleIllegalArgument(IllegalArgumentException exception) {
    return problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", exception.getMessage());
  }

  private static HttpStatus statusFor(String code) {
    return switch (code) {
      case "REVISION_CONFLICT" -> HttpStatus.PRECONDITION_FAILED;
      case "JOB_NOT_FOUND", "TRIGGER_NOT_FOUND", "DESTINATION_NOT_FOUND" -> HttpStatus.NOT_FOUND;
      case "IDEMPOTENCY_CONFLICT", "COMMAND_IN_PROGRESS", "COMMAND_PREVIOUSLY_FAILED" ->
          HttpStatus.CONFLICT;
      case "DESTINATION_DISABLED",
          "NAMESPACE_MISMATCH",
          "RESOURCE_DELETED",
          "TRIGGER_JOB_MISMATCH" -> HttpStatus.UNPROCESSABLE_ENTITY;
      default -> HttpStatus.BAD_REQUEST;
    };
  }

  private static ResponseEntity<ProblemDetail> problem(
      HttpStatus status, String code, String detail) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail == null ? code : detail);
    problem.setTitle("Scheduler request failed");
    problem.setType(
        URI.create(
            "urn:mcs:scheduler:error:"
                + code.toLowerCase(Locale.ROOT).replace('_', '-')));
    problem.setProperty("code", code);
    return ResponseEntity.status(status).body(problem);
  }
}
