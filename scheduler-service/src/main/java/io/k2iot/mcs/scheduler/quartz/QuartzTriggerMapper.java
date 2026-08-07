package io.k2iot.mcs.scheduler.quartz;

import io.k2iot.mcs.scheduler.trigger.CalendarIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.DailyTimeIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.SimpleIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.quartz.CalendarIntervalScheduleBuilder;
import org.quartz.CalendarIntervalTrigger;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.DailyTimeIntervalScheduleBuilder;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.DateBuilder;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public final class QuartzTriggerMapper {

  public Trigger toQuartz(TriggerDefinition definition, JobKey jobKey) {
    if (!definition.jobId().toString().equals(jobKey.getName())
        || !definition.namespace().equals(jobKey.getGroup())) {
      throw new IllegalArgumentException("Quartz job key must match trigger jobId and namespace");
    }
    if (definition.calendarNames().size() > 1) {
      throw new IllegalArgumentException("Quartz triggers can reference at most one Quartz calendar");
    }

    if (definition.spec() instanceof OnceTriggerSpec once) {
      return mapOnce(definition, jobKey, once);
    }
    if (definition.spec() instanceof CronTriggerSpec cron) {
      return mapCron(definition, jobKey, cron);
    }
    if (definition.spec() instanceof SimpleIntervalTriggerSpec simple) {
      return mapSimple(definition, jobKey, simple);
    }
    if (definition.spec() instanceof CalendarIntervalTriggerSpec calendar) {
      return mapCalendarInterval(definition, jobKey, calendar);
    }
    if (definition.spec() instanceof DailyTimeIntervalTriggerSpec daily) {
      return mapDailyInterval(definition, jobKey, daily);
    }
    throw new IllegalArgumentException(
        "Unsupported trigger specification: " + definition.spec().getClass().getName());
  }

  private Trigger mapOnce(
      TriggerDefinition definition, JobKey jobKey, OnceTriggerSpec specification) {
    SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0);
    schedule = applySimpleMisfire(schedule, definition.misfirePolicy());
    return build(definition, jobKey, schedule, specification.fireAt());
  }

  private Trigger mapCron(
      TriggerDefinition definition, JobKey jobKey, CronTriggerSpec specification) {
    CronScheduleBuilder schedule =
        CronScheduleBuilder.cronSchedule(specification.expression())
            .inTimeZone(TimeZone.getTimeZone(ZoneId.of(definition.timezone())));
    schedule = applyCronMisfire(schedule, definition.misfirePolicy());
    return build(definition, jobKey, schedule, definition.startAt());
  }

  private Trigger mapSimple(
      TriggerDefinition definition, JobKey jobKey, SimpleIntervalTriggerSpec specification) {
    SimpleScheduleBuilder schedule =
        SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(specification.interval().toMillis());
    if (specification.repeatCount() == null) {
      schedule = schedule.repeatForever();
    } else {
      schedule = schedule.withRepeatCount(Math.toIntExact(specification.repeatCount()));
    }
    schedule = applySimpleMisfire(schedule, definition.misfirePolicy());
    return build(definition, jobKey, schedule, definition.startAt());
  }

  private Trigger mapCalendarInterval(
      TriggerDefinition definition,
      JobKey jobKey,
      CalendarIntervalTriggerSpec specification) {
    CalendarIntervalScheduleBuilder schedule =
        CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
            .withInterval(specification.interval(), calendarIntervalUnit(specification.unit()))
            .inTimeZone(TimeZone.getTimeZone(ZoneId.of(definition.timezone())));
    schedule = applyCalendarMisfire(schedule, definition.misfirePolicy());
    return build(definition, jobKey, schedule, definition.startAt());
  }

  private Trigger mapDailyInterval(
      TriggerDefinition definition, JobKey jobKey, DailyTimeIntervalTriggerSpec specification) {
    requireDailyTimezoneCompatibleWithJvm(definition.timezone());

    Set<Integer> days =
        specification.daysOfWeek().stream().map(QuartzTriggerMapper::calendarDay).collect(Collectors.toSet());
    DailyTimeIntervalScheduleBuilder schedule =
        DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
            .withInterval(specification.interval(), dailyIntervalUnit(specification.unit()))
            .onDaysOfTheWeek(days)
            .startingDailyAt(timeOfDay(specification.startTime()))
            .endingDailyAt(timeOfDay(specification.endTime()));
    schedule = applyDailyMisfire(schedule, definition.misfirePolicy());
    return build(definition, jobKey, schedule, definition.startAt());
  }

  private <T extends Trigger> T build(
      TriggerDefinition definition,
      JobKey jobKey,
      ScheduleBuilder<T> schedule,
      java.time.Instant effectiveStart) {
    TriggerBuilder<T> builder =
        TriggerBuilder.newTrigger()
            .withIdentity(QuartzKeys.trigger(definition.triggerId(), definition.namespace()))
            .forJob(jobKey)
            .withDescription(definition.description())
            .withPriority(definition.priority())
            .withSchedule(schedule);

    if (effectiveStart == null) {
      builder = builder.startNow();
    } else {
      builder = builder.startAt(Date.from(effectiveStart));
    }
    if (definition.endAt() != null) {
      builder = builder.endAt(Date.from(definition.endAt()));
    }
    if (!definition.calendarNames().isEmpty()) {
      builder = builder.modifiedByCalendar(definition.calendarNames().iterator().next());
    }
    return builder.build();
  }

  private static SimpleScheduleBuilder applySimpleMisfire(
      SimpleScheduleBuilder schedule, TriggerDefinition.MisfirePolicy policy) {
    return switch (policy) {
      case SMART_POLICY -> schedule;
      case IGNORE_MISFIRES -> schedule.withMisfireHandlingInstructionIgnoreMisfires();
      case FIRE_NOW -> schedule.withMisfireHandlingInstructionFireNow();
      case RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT ->
          schedule.withMisfireHandlingInstructionNowWithExistingCount();
      case RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT ->
          schedule.withMisfireHandlingInstructionNowWithRemainingCount();
      case RESCHEDULE_NEXT_WITH_REMAINING_COUNT ->
          schedule.withMisfireHandlingInstructionNextWithRemainingCount();
      case RESCHEDULE_NEXT_WITH_EXISTING_COUNT ->
          schedule.withMisfireHandlingInstructionNextWithExistingCount();
      default -> throw unsupportedMisfire(policy, SimpleTrigger.class);
    };
  }

  private static CronScheduleBuilder applyCronMisfire(
      CronScheduleBuilder schedule, TriggerDefinition.MisfirePolicy policy) {
    return switch (policy) {
      case SMART_POLICY -> schedule;
      case IGNORE_MISFIRES -> schedule.withMisfireHandlingInstructionIgnoreMisfires();
      case FIRE_ONCE_NOW -> schedule.withMisfireHandlingInstructionFireAndProceed();
      case DO_NOTHING -> schedule.withMisfireHandlingInstructionDoNothing();
      default -> throw unsupportedMisfire(policy, CronTrigger.class);
    };
  }

  private static CalendarIntervalScheduleBuilder applyCalendarMisfire(
      CalendarIntervalScheduleBuilder schedule, TriggerDefinition.MisfirePolicy policy) {
    return switch (policy) {
      case SMART_POLICY -> schedule;
      case IGNORE_MISFIRES -> schedule.withMisfireHandlingInstructionIgnoreMisfires();
      case FIRE_ONCE_NOW -> schedule.withMisfireHandlingInstructionFireAndProceed();
      case DO_NOTHING -> schedule.withMisfireHandlingInstructionDoNothing();
      default -> throw unsupportedMisfire(policy, CalendarIntervalTrigger.class);
    };
  }

  private static DailyTimeIntervalScheduleBuilder applyDailyMisfire(
      DailyTimeIntervalScheduleBuilder schedule, TriggerDefinition.MisfirePolicy policy) {
    return switch (policy) {
      case SMART_POLICY -> schedule;
      case IGNORE_MISFIRES -> schedule.withMisfireHandlingInstructionIgnoreMisfires();
      case FIRE_ONCE_NOW -> schedule.withMisfireHandlingInstructionFireAndProceed();
      case DO_NOTHING -> schedule.withMisfireHandlingInstructionDoNothing();
      default -> throw unsupportedMisfire(policy, DailyTimeIntervalTrigger.class);
    };
  }

  private static DateBuilder.IntervalUnit calendarIntervalUnit(ChronoUnit unit) {
    return switch (unit) {
      case DAYS -> DateBuilder.IntervalUnit.DAY;
      case WEEKS -> DateBuilder.IntervalUnit.WEEK;
      case MONTHS -> DateBuilder.IntervalUnit.MONTH;
      case YEARS -> DateBuilder.IntervalUnit.YEAR;
      default -> throw new IllegalArgumentException("Unsupported calendar interval unit: " + unit);
    };
  }

  private static DateBuilder.IntervalUnit dailyIntervalUnit(ChronoUnit unit) {
    return switch (unit) {
      case SECONDS -> DateBuilder.IntervalUnit.SECOND;
      case MINUTES -> DateBuilder.IntervalUnit.MINUTE;
      case HOURS -> DateBuilder.IntervalUnit.HOUR;
      default -> throw new IllegalArgumentException("Unsupported daily interval unit: " + unit);
    };
  }

  private static int calendarDay(DayOfWeek dayOfWeek) {
    return dayOfWeek == DayOfWeek.SUNDAY ? Calendar.SUNDAY : dayOfWeek.getValue() + 1;
  }

  private static TimeOfDay timeOfDay(java.time.LocalTime time) {
    return new TimeOfDay(time.getHour(), time.getMinute(), time.getSecond());
  }

  private static void requireDailyTimezoneCompatibleWithJvm(String timezone) {
    ZoneId requested = ZoneId.of(timezone);
    ZoneId jvm = ZoneId.systemDefault();
    if (!requested.getRules().equals(jvm.getRules())) {
      throw new IllegalArgumentException(
          "Quartz DailyTimeIntervalTrigger has no per-trigger timezone; requested timezone "
              + requested
              + " does not match JVM timezone "
              + jvm);
    }
  }

  private static IllegalArgumentException unsupportedMisfire(
      TriggerDefinition.MisfirePolicy policy, Class<? extends Trigger> triggerType) {
    return new IllegalArgumentException(
        "Misfire policy " + policy + " is not supported by " + triggerType.getSimpleName());
  }
}
