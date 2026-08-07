package io.k2iot.mcs.scheduler.kafka;

import java.time.Instant;
import tools.jackson.databind.JsonNode;

public record SchedulerCommandEnvelope(
    int schemaVersion,
    String messageId,
    String requestId,
    Instant occurredAt,
    String producer,
    String namespace,
    String commandType,
    JsonNode payload) {}
