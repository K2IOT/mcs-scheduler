package io.k2iot.mcs.scheduler.quartz;

import io.k2iot.mcs.scheduler.execution.ScheduledExecutionService;
import java.util.Objects;
import org.quartz.JobExecutionContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class ConcurrentDispatchQuartzJob extends QuartzJobBean {

  private final ScheduledExecutionService scheduledExecutionService;

  public ConcurrentDispatchQuartzJob(ScheduledExecutionService scheduledExecutionService) {
    this.scheduledExecutionService =
        Objects.requireNonNull(scheduledExecutionService, "scheduledExecutionService");
  }

  @Override
  protected void executeInternal(JobExecutionContext context) {
    scheduledExecutionService.record(context);
  }
}
