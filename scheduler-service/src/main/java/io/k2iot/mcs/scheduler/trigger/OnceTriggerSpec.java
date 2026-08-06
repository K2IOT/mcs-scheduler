package io.k2iot.mcs.scheduler.trigger;

import java.time.Instant;
import java.util.Objects;

public record OnceTriggerSpec(Instant fireAt) implements TriggerSpec {

    public OnceTriggerSpec {
        Objects.requireNonNull(fireAt, "fireAt");
    }

    @Override
    public TriggerDefinition.Type type() {
        return TriggerDefinition.Type.ONCE;
    }
}
