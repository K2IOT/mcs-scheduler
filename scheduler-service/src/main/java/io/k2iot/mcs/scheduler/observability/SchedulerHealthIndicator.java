package io.k2iot.mcs.scheduler.observability;

import java.util.Objects;
import org.quartz.Scheduler;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.jdbc.core.simple.JdbcClient;

public final class SchedulerHealthIndicator implements HealthIndicator {

  private final Scheduler scheduler;
  private final JdbcClient jdbc;
  private final SchedulerMetrics metrics;

  public SchedulerHealthIndicator(Scheduler scheduler, JdbcClient jdbc, SchedulerMetrics metrics) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
    this.metrics = metrics;
  }

  @Override
  public Health health() {
    try {
      Integer databaseProbe = jdbc.sql("select 1").query(Integer.class).single();
      if (databaseProbe == null || databaseProbe != 1) {
        return Health.down().withDetail("database", "unavailable").build();
      }
      var metadata = scheduler.getMetaData();
      if (scheduler.isShutdown() || !scheduler.isStarted()) {
        return Health.down()
            .withDetail("scheduler", "not-started")
            .withDetail("schedulerName", scheduler.getSchedulerName())
            .build();
      }
      Long backlog =
          jdbc.sql(
                  """
                  select count(*)
                  from scheduler.outbox_event
                  where state in ('PENDING', 'IN_PROGRESS')
                  """)
              .query(Long.class)
              .single();
      long outboxBacklog = backlog == null ? 0L : backlog;
      if (metrics != null) {
        metrics.updateOutboxBacklog(outboxBacklog);
      }
      return Health.up()
          .withDetail("schedulerName", metadata.getSchedulerName())
          .withDetail("schedulerInstanceId", metadata.getSchedulerInstanceId())
          .withDetail("clustered", metadata.isJobStoreClustered())
          .withDetail("outboxBacklog", outboxBacklog)
          .build();
    } catch (Exception exception) {
      return Health.down(exception).build();
    }
  }
}
