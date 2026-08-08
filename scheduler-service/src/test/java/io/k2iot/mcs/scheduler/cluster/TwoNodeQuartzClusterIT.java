package io.k2iot.mcs.scheduler.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.execution.ExecutionIdentity;
import io.k2iot.mcs.scheduler.testing.ClusterTestApplication;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TwoNodeQuartzClusterIT {

  @Test
  void twoNodesCreateOneLogicalExecutionPerOccurrence() {
    try (var cluster = ClusterTestApplication.create()) {
      cluster.resetState();

      UUID destinationId = UUID.randomUUID();
      Set<UUID> expectedExecutionIds = new LinkedHashSet<>();
      Instant firstFireAt = Instant.now().plusSeconds(12).truncatedTo(ChronoUnit.MILLIS);

      try (var nodeA = cluster.startNode("cluster-a");
          var nodeB = cluster.startNode("cluster-b");
          var consumer = cluster.newExecutionConsumer()) {
        assertThat(nodeB.quartzInstanceId()).isNotEqualTo(nodeA.quartzInstanceId());
        cluster.registerKafkaDestination(nodeA, destinationId, "task14");

        for (int index = 0; index < 100; index++) {
          UUID jobId = UUID.randomUUID();
          UUID triggerId = UUID.randomUUID();
          Instant fireAt = firstFireAt.plusMillis((index % 20) * 200L);
          cluster.createOneShotSchedule(
              nodeA, jobId, triggerId, destinationId, "task14", fireAt);
          expectedExecutionIds.add(ExecutionIdentity.forScheduled(triggerId, fireAt));
        }

        assertThat(cluster.awaitKafkaExecutionIds(consumer, nodeA, 100))
            .containsExactlyInAnyOrderElementsOf(expectedExecutionIds);
        assertThat(cluster.executionCount(nodeA)).isEqualTo(100);
        assertThat(cluster.distinctExecutionCount(nodeA)).isEqualTo(100);
        assertThat(cluster.duplicateLogicalExecutionCount(nodeA)).isZero();
      }
    }
  }
}
