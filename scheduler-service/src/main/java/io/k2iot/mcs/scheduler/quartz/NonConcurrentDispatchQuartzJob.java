package io.k2iot.mcs.scheduler.quartz;

import io.k2iot.mcs.scheduler.execution.ScheduledExecutionService;
import org.quartz.DisallowConcurrentExecution;

@DisallowConcurrentExecution
public final class NonConcurrentDispatchQuartzJob extends ConcurrentDispatchQuartzJob {

  public NonConcurrentDispatchQuartzJob(ScheduledExecutionService scheduledExecutionService) {
    super(scheduledExecutionService);
  }
}
