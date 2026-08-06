package io.k2iot.mcs.scheduler.destination;

import java.util.Optional;
import java.util.UUID;

public interface DestinationRepository {

  Optional<DestinationDefinition> findByIdAndVersion(UUID destinationId, long version);
}
