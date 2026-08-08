package io.k2iot.mcs.scheduler.quartz;

import java.util.concurrent.CountDownLatch;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Test-profile-only hook used by the process-kill recovery acceptance drill. */
@Component
@Profile("cluster-recovery-test")
@ConditionalOnProperty(name = "mcs.scheduler.test.recovery-hook-enabled", havingValue = "true")
final class ProcessKillRecoveryHook {

  private final JdbcTemplate jdbc;
  private final String instanceId;

  ProcessKillRecoveryHook(
      JdbcTemplate jdbc, @Value("${mcs.scheduler.instance-id}") String instanceId) {
    this.jdbc = jdbc;
    this.instanceId = instanceId;
  }

  void beforeRecord(JobExecutionContext context) {
    if (context.isRecovering()) {
      return;
    }

    int inserted =
        jdbc.update(
            """
            insert into scheduler.process_kill_acquired_marker (
                fire_instance_id, instance_id, trigger_name, acquired_at)
            values (?, ?, ?, now())
            on conflict (fire_instance_id) do nothing
            """,
            context.getFireInstanceId(),
            instanceId,
            context.getTrigger().getKey().getName());

    if (inserted == 0) {
      return;
    }

    try {
      new CountDownLatch(1).await();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Process-kill recovery hook interrupted before node kill", exception);
    }
  }
}
