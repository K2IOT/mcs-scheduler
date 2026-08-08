package io.k2iot.mcs.scheduler.quartz;

import io.k2iot.mcs.scheduler.execution.ScheduledExecutionService;
import java.util.Objects;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class ConcurrentDispatchQuartzJob extends QuartzJobBean {

  private final ScheduledExecutionService scheduledExecutionService;
  private ProcessKillRecoveryHook processKillRecoveryHook;

  public ConcurrentDispatchQuartzJob(ScheduledExecutionService scheduledExecutionService) {
    this.scheduledExecutionService =
        Objects.requireNonNull(scheduledExecutionService, "scheduledExecutionService");
  }

  @Autowired(required = false)
  void setProcessKillRecoveryHook(ProcessKillRecoveryHook processKillRecoveryHook) {
    this.processKillRecoveryHook = processKillRecoveryHook;
  }

  @Override
  protected void executeInternal(JobExecutionContext context) {
    if (processKillRecoveryHook != null) {
      processKillRecoveryHook.beforeRecord(context);
    }
    scheduledExecutionService.record(context);
  }
}
