package io.k2iot.mcs.scheduler.job;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record JobDefinition(
        UUID jobId,
        String namespace,
        String name,
        String description,
        UUID destinationId,
        long destinationVersion,
        String eventType,
        Map<String, Object> payload,
        Map<String, String> headers,
        ConcurrencyPolicy concurrencyPolicy,
        RecoveryPolicy recoveryPolicy,
        boolean durable,
        State state,
        long revision,
        Instant createdAt,
        String createdBy,
        Instant updatedAt,
        String updatedBy) {

    public JobDefinition {
        Objects.requireNonNull(jobId, "jobId");
        namespace = requireText(namespace, "namespace");
        name = requireText(name, "name");
        Objects.requireNonNull(destinationId, "destinationId");
        if (destinationVersion < 1) {
            throw new IllegalArgumentException("destinationVersion must be positive");
        }
        eventType = requireText(eventType, "eventType");
        payload = Map.copyOf(Objects.requireNonNull(payload, "payload"));
        headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
        Objects.requireNonNull(concurrencyPolicy, "concurrencyPolicy");
        Objects.requireNonNull(recoveryPolicy, "recoveryPolicy");
        Objects.requireNonNull(state, "state");
        if (revision < 1) {
            throw new IllegalArgumentException("revision must be positive");
        }
        Objects.requireNonNull(createdAt, "createdAt");
        createdBy = requireText(createdBy, "createdBy");
        Objects.requireNonNull(updatedAt, "updatedAt");
        updatedBy = requireText(updatedBy, "updatedBy");
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public enum State {
        ACTIVE,
        PAUSED,
        DISABLED,
        DELETED
    }
}
