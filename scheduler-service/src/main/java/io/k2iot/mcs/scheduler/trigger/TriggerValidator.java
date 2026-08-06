package io.k2iot.mcs.scheduler.trigger;

import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.quartz.CronExpression;

public final class TriggerValidator {

    private static final Duration MINIMUM_INTERVAL = Duration.ofSeconds(1);
    private static final Pattern NAMESPACE_PATTERN =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
    private static final Pattern NAME_PATTERN =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,199}");

    private static final Set<TriggerDefinition.MisfirePolicy> COMMON_MISFIRE_POLICIES =
            EnumSet.of(
                    TriggerDefinition.MisfirePolicy.SMART_POLICY,
                    TriggerDefinition.MisfirePolicy.IGNORE_MISFIRES);

    public void validate(TriggerDefinition trigger, Instant now) {
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(now, "now");

        validateIdentity(trigger);
        validateWindow(trigger, now);
        validateMisfirePolicy(trigger);

        TriggerSpec spec = trigger.spec();
        if (spec instanceof OnceTriggerSpec once) {
            validateOnce(trigger, once, now);
        } else if (spec instanceof CronTriggerSpec cron) {
            validateCron(trigger, cron, now);
        } else if (spec instanceof SimpleIntervalTriggerSpec simple) {
            validateSimple(trigger, simple, now);
        } else if (spec instanceof CalendarIntervalTriggerSpec calendar) {
            validateCalendar(trigger, calendar, now);
        } else if (spec instanceof DailyTimeIntervalTriggerSpec daily) {
            validateDaily(trigger, daily, now);
        } else {
            throw invalidSpec("unsupported trigger specification: " + spec.getClass().getName());
        }
    }

    private static void validateIdentity(TriggerDefinition trigger) {
        if (!NAMESPACE_PATTERN.matcher(trigger.namespace()).matches()) {
            throw invalidSpec(
                    "namespace must be 1-64 characters using letters, digits, dot, underscore, or dash");
        }
        if (!NAME_PATTERN.matcher(trigger.name()).matches()) {
            throw invalidSpec(
                    "name must be 1-200 characters using letters, digits, dot, underscore, colon, or dash");
        }
        if (trigger.priority() < 1 || trigger.priority() > 10) {
            throw invalidSpec("priority must be between 1 and 10");
        }
        for (String calendarName : trigger.calendarNames()) {
            if (calendarName == null || calendarName.isBlank()) {
                throw invalidSpec("calendar names must not be blank");
            }
        }
    }

    private static void validateWindow(TriggerDefinition trigger, Instant now) {
        Instant startAt = trigger.startAt();
        Instant endAt = trigger.endAt();
        if (startAt != null && endAt != null && !endAt.isAfter(startAt)) {
            throw invalidSpec("endAt must be after startAt");
        }
        if (endAt != null && !endAt.isAfter(now)) {
            throw noFuture("schedule has no future fire time before endAt");
        }
    }

    private static void validateOnce(
            TriggerDefinition trigger, OnceTriggerSpec spec, Instant now) {
        Instant fireAt = spec.fireAt();
        if (!fireAt.isAfter(now)) {
            throw noFuture("once trigger fireAt must be in the future");
        }
        if (trigger.startAt() != null && fireAt.isBefore(trigger.startAt())) {
            throw invalidSpec("once trigger fireAt must not be before startAt");
        }
        if (trigger.endAt() != null && fireAt.isAfter(trigger.endAt())) {
            throw noFuture("once trigger fireAt must not be after endAt");
        }
    }

    private static void validateCron(
            TriggerDefinition trigger, CronTriggerSpec spec, Instant now) {
        ZoneId zone = requireTimezone(trigger);
        if (spec.expression().isBlank()) {
            throw invalidSpec("cron expression must not be blank");
        }

        CronExpression expression;
        try {
            expression = new CronExpression(spec.expression());
        } catch (ParseException exception) {
            throw new InvalidTriggerException(
                    "INVALID_TRIGGER_SPEC", "invalid Quartz cron expression", exception);
        }
        expression.setTimeZone(TimeZone.getTimeZone(zone));

        Instant cursor = now;
        if (trigger.startAt() != null && trigger.startAt().isAfter(now)) {
            cursor = trigger.startAt().minusMillis(1);
        }
        Date next = expression.getNextValidTimeAfter(Date.from(cursor));
        if (next == null
                || (trigger.endAt() != null && next.toInstant().isAfter(trigger.endAt()))) {
            throw noFuture("cron schedule has no future fire time");
        }
    }

    private static void validateSimple(
            TriggerDefinition trigger, SimpleIntervalTriggerSpec spec, Instant now) {
        Duration interval = spec.interval();
        if (interval.isNegative() || interval.isZero() || interval.compareTo(MINIMUM_INTERVAL) < 0) {
            throw invalidSpec("simple interval must be at least one second");
        }
        if (spec.repeatCount() != null && spec.repeatCount() < 0) {
            throw invalidSpec("simple interval repeatCount must be zero or positive");
        }
        ensureWindowContainsFuture(trigger, now);
    }

    private static void validateCalendar(
            TriggerDefinition trigger, CalendarIntervalTriggerSpec spec, Instant now) {
        requireTimezone(trigger);
        if (spec.interval() < 1) {
            throw invalidSpec("calendar interval must be positive");
        }
        if (spec.unit() != ChronoUnit.DAYS
                && spec.unit() != ChronoUnit.WEEKS
                && spec.unit() != ChronoUnit.MONTHS
                && spec.unit() != ChronoUnit.YEARS) {
            throw invalidSpec("calendar interval unit must be days, weeks, months, or years");
        }
        ensureWindowContainsFuture(trigger, now);
    }

    private static void validateDaily(
            TriggerDefinition trigger, DailyTimeIntervalTriggerSpec spec, Instant now) {
        ZoneId zone = requireTimezone(trigger);
        if (spec.interval() < 1) {
            throw invalidSpec("daily interval must be positive");
        }
        if (spec.unit() != ChronoUnit.SECONDS
                && spec.unit() != ChronoUnit.MINUTES
                && spec.unit() != ChronoUnit.HOURS) {
            throw invalidSpec("daily interval unit must be seconds, minutes, or hours");
        }

        Duration interval;
        try {
            interval = spec.unit().getDuration().multipliedBy(spec.interval());
        } catch (ArithmeticException exception) {
            throw new InvalidTriggerException(
                    "INVALID_TRIGGER_SPEC", "daily interval is too large", exception);
        }
        if (interval.compareTo(MINIMUM_INTERVAL) < 0) {
            throw invalidSpec("daily interval must be at least one second");
        }
        if (spec.daysOfWeek().isEmpty()) {
            throw invalidSpec("daily interval requires at least one weekday");
        }
        if (!spec.endTime().isAfter(spec.startTime())) {
            throw invalidSpec("daily interval endTime must be after startTime");
        }
        if (!hasFutureDailyFire(trigger, spec, zone, now, interval)) {
            throw noFuture("daily interval schedule has no future fire time");
        }
    }

    private static boolean hasFutureDailyFire(
            TriggerDefinition trigger,
            DailyTimeIntervalTriggerSpec spec,
            ZoneId zone,
            Instant now,
            Duration interval) {
        Instant effectiveStart = now;
        if (trigger.startAt() != null && trigger.startAt().isAfter(effectiveStart)) {
            effectiveStart = trigger.startAt();
        }

        ZonedDateTime from = effectiveStart.atZone(zone);
        for (int dayOffset = 0; dayOffset <= 7; dayOffset++) {
            LocalDate date = from.toLocalDate().plusDays(dayOffset);
            if (!spec.daysOfWeek().contains(date.getDayOfWeek())) {
                continue;
            }

            LocalTime candidateTime = spec.startTime();
            if (dayOffset == 0 && !from.toLocalTime().isBefore(candidateTime)) {
                LocalDateTime next = from.toLocalDateTime().plus(interval);
                if (!next.toLocalDate().equals(date)) {
                    continue;
                }
                candidateTime = next.toLocalTime();
            }
            if (candidateTime.isAfter(spec.endTime())) {
                continue;
            }

            Instant candidate = ZonedDateTime.of(date, candidateTime, zone).toInstant();
            if (!candidate.isAfter(now)) {
                continue;
            }
            if (trigger.endAt() == null || !candidate.isAfter(trigger.endAt())) {
                return true;
            }
        }
        return false;
    }

    private static void ensureWindowContainsFuture(TriggerDefinition trigger, Instant now) {
        Instant next = now.plusSeconds(1);
        if (trigger.startAt() != null && trigger.startAt().isAfter(next)) {
            next = trigger.startAt();
        }
        if (trigger.endAt() != null && next.isAfter(trigger.endAt())) {
            throw noFuture("schedule has no future fire time");
        }
    }

    private static ZoneId requireTimezone(TriggerDefinition trigger) {
        if (trigger.timezone() == null || trigger.timezone().isBlank()) {
            throw invalidSpec(trigger.type() + " trigger requires an IANA timezone");
        }
        try {
            return ZoneId.of(trigger.timezone());
        } catch (DateTimeException exception) {
            throw new InvalidTriggerException(
                    "INVALID_TRIGGER_SPEC", "invalid IANA timezone: " + trigger.timezone(), exception);
        }
    }

    private static void validateMisfirePolicy(TriggerDefinition trigger) {
        Set<TriggerDefinition.MisfirePolicy> allowed = EnumSet.copyOf(COMMON_MISFIRE_POLICIES);
        switch (trigger.type()) {
            case ONCE -> allowed.add(TriggerDefinition.MisfirePolicy.FIRE_NOW);
            case CRON, CALENDAR_INTERVAL, DAILY_TIME_INTERVAL -> {
                allowed.add(TriggerDefinition.MisfirePolicy.FIRE_ONCE_NOW);
                allowed.add(TriggerDefinition.MisfirePolicy.DO_NOTHING);
            }
            case SIMPLE_INTERVAL -> {
                allowed.add(TriggerDefinition.MisfirePolicy.FIRE_NOW);
                allowed.add(
                        TriggerDefinition.MisfirePolicy
                                .RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT);
                allowed.add(
                        TriggerDefinition.MisfirePolicy
                                .RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT);
                allowed.add(
                        TriggerDefinition.MisfirePolicy.RESCHEDULE_NEXT_WITH_REMAINING_COUNT);
                allowed.add(
                        TriggerDefinition.MisfirePolicy.RESCHEDULE_NEXT_WITH_EXISTING_COUNT);
            }
        }
        if (!allowed.contains(trigger.misfirePolicy())) {
            throw new InvalidTriggerException(
                    "INVALID_MISFIRE_POLICY",
                    "misfire policy "
                            + trigger.misfirePolicy()
                            + " is not supported for "
                            + trigger.type());
        }
    }

    private static InvalidTriggerException invalidSpec(String message) {
        return new InvalidTriggerException("INVALID_TRIGGER_SPEC", message);
    }

    private static InvalidTriggerException noFuture(String message) {
        return new InvalidTriggerException("SCHEDULE_HAS_NO_FUTURE_FIRE_TIME", message);
    }
}
