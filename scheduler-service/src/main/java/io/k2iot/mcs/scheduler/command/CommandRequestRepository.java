package io.k2iot.mcs.scheduler.command;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

public interface CommandRequestRepository {

  Optional<CommandRequest> findByRequestId(UUID requestId);

  void insert(CommandRequest request);

  void complete(UUID requestId, JsonNode responseJson, Instant processedAt);
}
