package io.k2iot.mcs.scheduler.trigger;

import java.util.Objects;

public record CronTriggerSpec(String expression) implements TriggerSpec {

  public CronTriggerSpec {
    Objects.requireNonNull(expression, "expression");
  }

  @Override
  public TriggerDefinition.Type type() {
    return TriggerDefinition.Type.CRON;
  }
}
