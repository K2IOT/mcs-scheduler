package io.k2iot.mcs.scheduler.command;

import java.util.Objects;

public final class SchedulerCommandException extends RuntimeException {

  private final String code;

  public SchedulerCommandException(String code, String message) {
    super(message);
    this.code = requireCode(code);
  }

  public SchedulerCommandException(String code, String message, Throwable cause) {
    super(message, cause);
    this.code = requireCode(code);
  }

  public String code() {
    return code;
  }

  private static String requireCode(String code) {
    Objects.requireNonNull(code, "code");
    if (code.isBlank()) {
      throw new IllegalArgumentException("code must not be blank");
    }
    return code;
  }
}
