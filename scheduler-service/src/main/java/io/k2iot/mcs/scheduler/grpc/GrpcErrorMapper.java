package io.k2iot.mcs.scheduler.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.trigger.InvalidTriggerException;
import org.springframework.stereotype.Component;

@Component
public final class GrpcErrorMapper {

  static final Metadata.Key<String> ERROR_CODE_TRAILER =
      Metadata.Key.of("scheduler-error-code", Metadata.ASCII_STRING_MARSHALLER);

  public StatusRuntimeException toStatusRuntimeException(RuntimeException exception) {
    if (exception instanceof StatusRuntimeException statusRuntimeException) {
      return statusRuntimeException;
    }

    String code = codeFor(exception);
    Status status = statusFor(code, exception).withDescription(description(exception, code));
    Metadata trailers = new Metadata();
    trailers.put(ERROR_CODE_TRAILER, code);
    return status.asRuntimeException(trailers);
  }

  private static String codeFor(RuntimeException exception) {
    if (exception instanceof SchedulerCommandException schedulerException) {
      return schedulerException.code();
    }
    if (exception instanceof InvalidTriggerException triggerException) {
      return triggerException.code();
    }
    if (exception instanceof IllegalArgumentException) {
      return "INVALID_REQUEST";
    }
    return "INTERNAL_ERROR";
  }

  private static Status statusFor(String code, RuntimeException exception) {
    if (!(exception instanceof SchedulerCommandException)
        && !(exception instanceof InvalidTriggerException)
        && !(exception instanceof IllegalArgumentException)) {
      return Status.INTERNAL;
    }
    return switch (code) {
      case "REVISION_CONFLICT", "COMMAND_IN_PROGRESS" -> Status.ABORTED;
      case "JOB_NOT_FOUND", "TRIGGER_NOT_FOUND", "DESTINATION_NOT_FOUND" -> Status.NOT_FOUND;
      case "IDEMPOTENCY_CONFLICT" -> Status.ALREADY_EXISTS;
      case "COMMAND_PREVIOUSLY_FAILED",
              "DESTINATION_DISABLED",
              "NAMESPACE_MISMATCH",
              "RESOURCE_DELETED",
              "TRIGGER_JOB_MISMATCH" ->
          Status.FAILED_PRECONDITION;
      default -> Status.INVALID_ARGUMENT;
    };
  }

  private static String description(RuntimeException exception, String code) {
    return exception.getMessage() == null || exception.getMessage().isBlank()
        ? code
        : exception.getMessage();
  }
}
