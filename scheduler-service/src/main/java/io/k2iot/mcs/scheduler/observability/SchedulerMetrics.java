package io.k2iot.mcs.scheduler.observability;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class SchedulerMetrics {

  private final MeterRegistry registry;
  private final AtomicLong outboxBacklog = new AtomicLong();

  public SchedulerMetrics(MeterRegistry registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
    registry.gauge("mcs.scheduler.outbox.backlog", outboxBacklog);
  }

  public void recordFinding(QuartzReconciler.FindingType type) {
    registry.counter("mcs.scheduler.reconciliation.findings", "type", type.name()).increment();
  }

  public void recordRepair() {
    registry.counter("mcs.scheduler.reconciliation.repairs").increment();
  }

  public void updateOutboxBacklog(long backlog) {
    outboxBacklog.set(Math.max(0, backlog));
  }
}
