package io.k2iot.mcs.scheduler.quartz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.k2iot.mcs.scheduler.trigger.CalendarIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.DailyTimeIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.OnceTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.SimpleIntervalTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerSpec;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.quartz.CalendarIntervalTrigger;
import org.quartz.CronTrigger;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.DateBuilder;
import org.quartz.JobKey;
import org.quartz.SimpleTrigger;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;

class QuartzTriggerMapperTest {

  private static final Instant NOW = Instant.parse("2026-08-07T01:00:00Z");
  private static final UUID TRIGGER_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
  private static final UUID JOB_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
  private static final JobKey JOB_KEY = QuartzKeys.job(JOB_ID, "billing");

  private final QuartzTriggerMapper mapper = new QuartzTriggerMapper();

  @Test
  void createsStableQuartzKeysFromDomainIdentifiers() {
    assertThat(QuartzKeys.job(JOB_ID, "billing"))
        .extracting(JobKey::getName, JobKey::getGroup)
        .containsExactly(JOB_ID.toString(), "billing");
    assertThat(QuartzKeys.trigger(TRIGGER_ID, "billing").getName()).isEqualTo(TRIGGER_ID.toString());
    assertThat(QuartzKeys.trigger(TRIGGER_ID, "billing").getGroup()).isEqualTo("billing");
  }

  @Test
  void mapsOnceTriggerToSingleFireAtRequestedInstant() {
    Instant fireAt = NOW.plusSeconds(120);

    Trigger trigger =
        mapper.toQuartz(
            definition(
                new OnceTriggerSpec(fireAt),
                null,
                TriggerDefinition.MisfirePolicy.FIRE_NOW,
                Set.of()),
            JOB_KEY);

    assertThat(trigger).isInstanceOf(SimpleTrigger.class);
    SimpleTrigger simple = (SimpleTrigger) trigger;
    assertThat(simple.getStartTime()).isEqualTo(Date.from(fireAt));
    assertThat(simple.getRepeatCount()).isZero();
    assertThat(simple.getMisfireInstruction()).isEqualTo(SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW);
  }

  @Test
  void mapsCronTimezoneAndExplicitMisfireInstruction() {
    Trigger trigger =
        mapper.toQuartz(
            definition(
                new CronTriggerSpec("0 0 8 * * ?"),
                "Asia/Ho_Chi_Minh",
                TriggerDefinition.MisfirePolicy.DO_NOTHING,
                Set.of()),
            JOB_KEY);

    assertThat(trigger).isInstanceOf(CronTrigger.class);
    CronTrigger cron = (CronTrigger) trigger;
    assertThat(cron.getCronExpression()).isEqualTo("0 0 8 * * ?");
    assertThat(cron.getTimeZone().getID()).isEqualTo("Asia/Ho_Chi_Minh");
    assertThat(cron.getMisfireInstruction()).isEqualTo(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING);
  }

  @Test
  void mapsSimpleIntervalAndPreservesRepeatCount() {
    Trigger trigger =
        mapper.toQuartz(
            definition(
                new SimpleIntervalTriggerSpec(Duration.ofSeconds(5), 7L),
                null,
                TriggerDefinition.MisfirePolicy.RESCHEDULE_NEXT_WITH_REMAINING_COUNT,
                Set.of()),
            JOB_KEY);

    assertThat(trigger).isInstanceOf(SimpleTrigger.class);
    SimpleTrigger simple = (SimpleTrigger) trigger;
    assertThat(simple.getRepeatInterval()).isEqualTo(5_000L);
    assertThat(simple.getRepeatCount()).isEqualTo(7);
    assertThat(simple.getMisfireInstruction())
        .isEqualTo(SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT);
  }

  @Test
  void mapsCalendarIntervalMonthsWithoutConvertingToSeconds() {
    Trigger trigger =
        mapper.toQuartz(
            definition(
                new CalendarIntervalTriggerSpec(6, ChronoUnit.MONTHS),
                "Asia/Ho_Chi_Minh",
                TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW,
                Set.of()),
            JOB_KEY);

    assertThat(trigger).isInstanceOf(CalendarIntervalTrigger.class);
    CalendarIntervalTrigger calendar = (CalendarIntervalTrigger) trigger;
    assertThat(calendar.getRepeatIntervalUnit()).isEqualTo(DateBuilder.IntervalUnit.MONTH);
    assertThat(calendar.getRepeatInterval()).isEqualTo(6);
    assertThat(calendar.getTimeZone().getID()).isEqualTo("Asia/Ho_Chi_Minh");
    assertThat(calendar.getMisfireInstruction())
        .isEqualTo(CalendarIntervalTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW);
  }

  @Test
  void mapsDailyIntervalWeekdaysAndDailyWindow() {
    Trigger trigger =
        mapper.toQuartz(
            definition(
                new DailyTimeIntervalTriggerSpec(
                    15,
                    ChronoUnit.MINUTES,
                    Set.of(DayOfWeek.MONDAY, DayOfWeek.WEDNESDAY),
                    LocalTime.of(8, 30),
                    LocalTime.of(18, 15)),
                "Asia/Ho_Chi_Minh",
                TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW,
                Set.of()),
            JOB_KEY);

    assertThat(trigger).isInstanceOf(DailyTimeIntervalTrigger.class);
    DailyTimeIntervalTrigger daily = (DailyTimeIntervalTrigger) trigger;
    assertThat(daily.getRepeatIntervalUnit()).isEqualTo(DateBuilder.IntervalUnit.MINUTE);
    assertThat(daily.getRepeatInterval()).isEqualTo(15);
    assertThat(daily.getDaysOfWeek()).containsExactlyInAnyOrder(Calendar.MONDAY, Calendar.WEDNESDAY);
    assertThat(daily.getStartTimeOfDay()).isEqualTo(new TimeOfDay(8, 30, 0));
    assertThat(daily.getEndTimeOfDay()).isEqualTo(new TimeOfDay(18, 15, 0));
    assertThat(daily.getMisfireInstruction())
        .isEqualTo(DailyTimeIntervalTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW);
  }

  @Test
  void mapsCommonMetadataAndOneQuartzCalendar() {
    Trigger trigger =
        mapper.toQuartz(
            definition(
                new SimpleIntervalTriggerSpec(Duration.ofMinutes(5), null),
                null,
                TriggerDefinition.MisfirePolicy.IGNORE_MISFIRES,
                Set.of("vietnam-holidays")),
            JOB_KEY);

    assertThat(trigger.getKey()).isEqualTo(QuartzKeys.trigger(TRIGGER_ID, "billing"));
    assertThat(trigger.getJobKey()).isEqualTo(JOB_KEY);
    assertThat(trigger.getDescription()).isEqualTo("renewal trigger");
    assertThat(trigger.getPriority()).isEqualTo(7);
    assertThat(trigger.getStartTime()).isEqualTo(Date.from(NOW.plusSeconds(30)));
    assertThat(trigger.getEndTime()).isEqualTo(Date.from(NOW.plusDays(30)));
    assertThat(trigger.getCalendarName()).isEqualTo("vietnam-holidays");
  }

  @Test
  void rejectsMultipleDomainCalendarsBecauseQuartzSupportsOnlyOneCalendarReference() {
    TriggerDefinition definition =
        definition(
            new SimpleIntervalTriggerSpec(Duration.ofMinutes(5), null),
            null,
            TriggerDefinition.MisfirePolicy.SMART_POLICY,
            Set.of("holidays", "maintenance"));

    assertThatThrownBy(() -> mapper.toQuartz(definition, JOB_KEY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one Quartz calendar");
  }

  private static TriggerDefinition definition(
      TriggerSpec spec,
      String timezone,
      TriggerDefinition.MisfirePolicy misfirePolicy,
      Set<String> calendars) {
    return new TriggerDefinition(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "renewal",
        "renewal trigger",
        spec,
        NOW.plusSeconds(30),
        NOW.plusDays(30),
        7,
        timezone,
        misfirePolicy,
        calendars,
        TriggerDefinition.State.ACTIVE,
        3,
        NOW.minusSeconds(300),
        "test",
        NOW,
        "test");
  }
}
