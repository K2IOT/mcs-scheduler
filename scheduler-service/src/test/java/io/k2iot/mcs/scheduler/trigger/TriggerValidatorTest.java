package io.k2iot.mcs.scheduler.trigger;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TriggerValidatorTest {

    private static final Instant NOW = Instant.parse("2026-08-06T00:00:00Z");
    private static final UUID TRIGGER_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    private static final UUID JOB_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");

    private final TriggerValidator validator = new TriggerValidator();

    @Test
    void rejectsCronWithoutTimezone() {
        TriggerDefinition trigger =
                trigger(
                        new CronTriggerSpec("0 0 8 * * ?"),
                        null,
                        TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("timezone");
    }

    @Test
    void rejectsInvalidCronExpression() {
        TriggerDefinition trigger =
                trigger(
                        new CronTriggerSpec("not a cron expression"),
                        "Asia/Ho_Chi_Minh",
                        TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("cron");
    }

    @Test
    void rejectsInvalidTimezone() {
        TriggerDefinition trigger =
                trigger(
                        new CronTriggerSpec("0 0 8 * * ?"),
                        "Mars/Olympus_Mons",
                        TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("timezone");
    }

    @Test
    void rejectsSimpleIntervalBelowOneSecond() {
        TriggerDefinition trigger =
                trigger(
                        new SimpleIntervalTriggerSpec(Duration.ofMillis(999), null),
                        null,
                        TriggerDefinition.MisfirePolicy.FIRE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("one second");
    }

    @Test
    void rejectsEndAtThatIsNotAfterStartAt() {
        TriggerDefinition trigger =
                new TriggerDefinition(
                        TRIGGER_ID,
                        JOB_ID,
                        "billing",
                        "renewal",
                        null,
                        new SimpleIntervalTriggerSpec(Duration.ofSeconds(5), null),
                        NOW.plusSeconds(30),
                        NOW.plusSeconds(30),
                        5,
                        null,
                        TriggerDefinition.MisfirePolicy.FIRE_NOW,
                        Set.of(),
                        TriggerDefinition.State.ACTIVE,
                        1,
                        NOW,
                        "test",
                        NOW,
                        "test");

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("endAt");
    }

    @Test
    void rejectsUnsupportedCalendarIntervalUnit() {
        TriggerDefinition trigger =
                trigger(
                        new CalendarIntervalTriggerSpec(1, ChronoUnit.HOURS),
                        "Asia/Ho_Chi_Minh",
                        TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("days, weeks, months, or years");
    }

    @Test
    void rejectsDailyIntervalWithoutWeekdays() {
        TriggerDefinition trigger =
                trigger(
                        new DailyTimeIntervalTriggerSpec(
                                15,
                                ChronoUnit.MINUTES,
                                Set.of(),
                                LocalTime.of(8, 0),
                                LocalTime.of(18, 0)),
                        "Asia/Ho_Chi_Minh",
                        TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("weekday");
    }

    @Test
    void rejectsMisfirePolicyThatDoesNotBelongToTriggerType() {
        TriggerDefinition trigger =
                trigger(
                        new CronTriggerSpec("0 0 8 * * ?"),
                        "Asia/Ho_Chi_Minh",
                        TriggerDefinition.MisfirePolicy.RESCHEDULE_NEXT_WITH_EXISTING_COUNT);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("misfire");
    }

    @Test
    void rejectsOnceTriggerWithoutFutureFireTime() {
        TriggerDefinition trigger =
                trigger(
                        new OnceTriggerSpec(NOW),
                        null,
                        TriggerDefinition.MisfirePolicy.FIRE_NOW);

        assertThatThrownBy(() -> validator.validate(trigger, NOW))
                .isInstanceOf(InvalidTriggerException.class)
                .hasMessageContaining("future");
    }

    @Test
    void acceptsSupportedTriggerVariants() {
        assertThatCode(
                        () ->
                                validator.validate(
                                        trigger(
                                                new OnceTriggerSpec(NOW.plusSeconds(60)),
                                                null,
                                                TriggerDefinition.MisfirePolicy.FIRE_NOW),
                                        NOW))
                .doesNotThrowAnyException();

        assertThatCode(
                        () ->
                                validator.validate(
                                        trigger(
                                                new CronTriggerSpec("0 0 8 * * ?"),
                                                "Asia/Ho_Chi_Minh",
                                                TriggerDefinition.MisfirePolicy.DO_NOTHING),
                                        NOW))
                .doesNotThrowAnyException();

        assertThatCode(
                        () ->
                                validator.validate(
                                        trigger(
                                                new SimpleIntervalTriggerSpec(
                                                        Duration.ofSeconds(5), 10L),
                                                null,
                                                TriggerDefinition.MisfirePolicy
                                                        .RESCHEDULE_NEXT_WITH_REMAINING_COUNT),
                                        NOW))
                .doesNotThrowAnyException();

        assertThatCode(
                        () ->
                                validator.validate(
                                        trigger(
                                                new CalendarIntervalTriggerSpec(
                                                        1, ChronoUnit.MONTHS),
                                                "Asia/Ho_Chi_Minh",
                                                TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW),
                                        NOW))
                .doesNotThrowAnyException();

        assertThatCode(
                        () ->
                                validator.validate(
                                        trigger(
                                                new DailyTimeIntervalTriggerSpec(
                                                        15,
                                                        ChronoUnit.MINUTES,
                                                        Set.of(
                                                                DayOfWeek.MONDAY,
                                                                DayOfWeek.TUESDAY),
                                                        LocalTime.of(8, 0),
                                                        LocalTime.of(18, 0)),
                                                "Asia/Ho_Chi_Minh",
                                                TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW),
                                        NOW))
                .doesNotThrowAnyException();
    }

    private static TriggerDefinition trigger(
            TriggerSpec spec,
            String timezone,
            TriggerDefinition.MisfirePolicy misfirePolicy) {
        return new TriggerDefinition(
                TRIGGER_ID,
                JOB_ID,
                "billing",
                "renewal",
                null,
                spec,
                NOW.plusSeconds(10),
                null,
                5,
                timezone,
                misfirePolicy,
                Set.of(),
                TriggerDefinition.State.ACTIVE,
                1,
                NOW,
                "test",
                NOW,
                "test");
    }
}
