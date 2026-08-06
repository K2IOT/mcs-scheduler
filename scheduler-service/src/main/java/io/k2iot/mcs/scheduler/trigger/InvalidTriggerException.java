package io.k2iot.mcs.scheduler.trigger;

import java.util.Objects;

public final class InvalidTriggerException extends IllegalArgumentException {

    private final String code;

    public InvalidTriggerException(String code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public InvalidTriggerException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    public String code() {
        return code;
    }
}
