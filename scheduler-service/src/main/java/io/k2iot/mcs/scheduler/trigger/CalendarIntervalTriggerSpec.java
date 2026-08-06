package io.k2iot.mcs.scheduler.trigger;

import java.time.temporal.ChronoUnit;
import java.util.Objects;

public record CalendarIntervalTriggerSpec(int interval, ChronoUnit unit) implements TriggerSpec {

  public CalendarIntervalTriggerSpec {
    Objects.requireNonNull(unit, "unit");
  }

  @Override
  public TriggerDefinition.Type type() {
    return TriggerDefinition.Type.CALENDAR_INTERVAL;
  }
}
