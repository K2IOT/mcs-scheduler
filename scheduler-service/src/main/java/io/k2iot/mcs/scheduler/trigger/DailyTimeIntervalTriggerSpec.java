package io.k2iot.mcs.scheduler.trigger;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Set;

public record DailyTimeIntervalTriggerSpec(
    int interval,
    ChronoUnit unit,
    Set<DayOfWeek> daysOfWeek,
    LocalTime startTime,
    LocalTime endTime)
    implements TriggerSpec {

  public DailyTimeIntervalTriggerSpec {
    Objects.requireNonNull(unit, "unit");
    daysOfWeek = Set.copyOf(Objects.requireNonNull(daysOfWeek, "daysOfWeek"));
    Objects.requireNonNull(startTime, "startTime");
    Objects.requireNonNull(endTime, "endTime");
  }

  @Override
  public TriggerDefinition.Type type() {
    return TriggerDefinition.Type.DAILY_TIME_INTERVAL;
  }
}
