package io.k2iot.mcs.scheduler.trigger;

public sealed interface TriggerSpec
        permits OnceTriggerSpec,
                CronTriggerSpec,
                SimpleIntervalTriggerSpec,
                CalendarIntervalTriggerSpec,
                DailyTimeIntervalTriggerSpec {

    TriggerDefinition.Type type();
}
