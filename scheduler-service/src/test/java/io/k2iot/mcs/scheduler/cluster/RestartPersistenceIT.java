package io.k2iot.mcs.scheduler.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.execution.ExecutionIdentity;
import io.k2iot.mcs.scheduler.testing.ClusterTestApplication;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RestartPersistenceIT {

  @Test
  void futureOneShotSurvivesNodeRestartAndPublishesAfterDueTime() {
    try (var cluster = ClusterTestApplication.create()) {
      cluster.resetState();

      UUID destinationId = UUID.randomUUID();
      UUID jobId = UUID.randomUUID();
      UUID triggerId = UUID.randomUUID();

      String nodeAQuartzInstanceId;
      Instant fireAt;
      try (var nodeA = cluster.startNode("restart-a")) {
        nodeAQuartzInstanceId = nodeA.quartzInstanceId();
        cluster.registerKafkaDestination(nodeA, destinationId, "task14");
        fireAt = Instant.now().plusSeconds(10).truncatedTo(ChronoUnit.MILLIS);
        cluster.createOneShotSchedule(nodeA, jobId, triggerId, destinationId, "task14", fireAt);
        assertThat(Instant.now()).isBefore(fireAt);
      }

      try (var consumer = cluster.newExecutionConsumer();
          var nodeB = cluster.startNode("restart-b")) {
        assertThat(nodeB.quartzInstanceId()).isNotEqualTo(nodeAQuartzInstanceId);

        UUID expectedExecutionId = ExecutionIdentity.forScheduled(triggerId, fireAt);
        assertThat(cluster.awaitKafkaExecutionIds(consumer, nodeB, 1))
            .containsExactly(expectedExecutionId);
        assertThat(Instant.now()).isAfterOrEqualTo(fireAt);
        assertThat(cluster.executionCount(nodeB)).isEqualTo(1);
        assertThat(cluster.executionCount(nodeB, expectedExecutionId)).isEqualTo(1);
      }
    }
  }
}
