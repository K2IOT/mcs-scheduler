package io.k2iot.mcs.scheduler.trigger;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = OnceTriggerSpec.class, name = "ONCE"),
  @JsonSubTypes.Type(value = CronTriggerSpec.class, name = "CRON"),
  @JsonSubTypes.Type(value = SimpleIntervalTriggerSpec.class, name = "SIMPLE_INTERVAL"),
  @JsonSubTypes.Type(value = CalendarIntervalTriggerSpec.class, name = "CALENDAR_INTERVAL"),
  @JsonSubTypes.Type(value = DailyTimeIntervalTriggerSpec.class, name = "DAILY_TIME_INTERVAL")
})
public sealed interface TriggerSpec
    permits OnceTriggerSpec,
        CronTriggerSpec,
        SimpleIntervalTriggerSpec,
        CalendarIntervalTriggerSpec,
        DailyTimeIntervalTriggerSpec {

  TriggerDefinition.Type type();
}
