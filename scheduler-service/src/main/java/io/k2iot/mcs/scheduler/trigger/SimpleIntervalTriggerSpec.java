package io.k2iot.mcs.scheduler.trigger;

import java.time.Duration;
import java.util.Objects;

public record SimpleIntervalTriggerSpec(Duration interval, Long repeatCount)
        implements TriggerSpec {

    public SimpleIntervalTriggerSpec {
        Objects.requireNonNull(interval, "interval");
    }

    @Override
    public TriggerDefinition.Type type() {
        return TriggerDefinition.Type.SIMPLE_INTERVAL;
    }
}
