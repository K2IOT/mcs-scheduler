package io.k2iot.mcs.scheduler.trigger;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record TriggerDefinition(
    UUID triggerId,
    UUID jobId,
    String namespace,
    String name,
    String description,
    TriggerSpec spec,
    Instant startAt,
    Instant endAt,
    int priority,
    String timezone,
    MisfirePolicy misfirePolicy,
    Set<String> calendarNames,
    State state,
    long revision,
    Instant createdAt,
    String createdBy,
    Instant updatedAt,
    String updatedBy) {

  public TriggerDefinition {
    Objects.requireNonNull(triggerId, "triggerId");
    Objects.requireNonNull(jobId, "jobId");
    namespace = requireText(namespace, "namespace");
    name = requireText(name, "name");
    Objects.requireNonNull(spec, "spec");
    Objects.requireNonNull(misfirePolicy, "misfirePolicy");
    calendarNames = Set.copyOf(Objects.requireNonNull(calendarNames, "calendarNames"));
    Objects.requireNonNull(state, "state");
    if (revision < 1) {
      throw new IllegalArgumentException("revision must be positive");
    }
    Objects.requireNonNull(createdAt, "createdAt");
    createdBy = requireText(createdBy, "createdBy");
    Objects.requireNonNull(updatedAt, "updatedAt");
    updatedBy = requireText(updatedBy, "updatedBy");
  }

  public Type type() {
    return spec.type();
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }

  public enum Type {
    ONCE,
    CRON,
    SIMPLE_INTERVAL,
    CALENDAR_INTERVAL,
    DAILY_TIME_INTERVAL
  }

  public enum State {
    ACTIVE,
    PAUSED,
    DISABLED,
    DELETED
  }

  public enum MisfirePolicy {
    SMART_POLICY,
    IGNORE_MISFIRES,
    FIRE_NOW,
    FIRE_ONCE_NOW,
    DO_NOTHING,
    RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT,
    RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT,
    RESCHEDULE_NEXT_WITH_REMAINING_COUNT,
    RESCHEDULE_NEXT_WITH_EXISTING_COUNT
  }
}
