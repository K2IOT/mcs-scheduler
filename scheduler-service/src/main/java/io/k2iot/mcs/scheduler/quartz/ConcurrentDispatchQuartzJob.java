package io.k2iot.mcs.scheduler.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class ConcurrentDispatchQuartzJob extends QuartzJobBean {

  @Override
  protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    String jobId = context.getMergedJobDataMap().getString(QuartzKeys.JOB_ID);
    throw new JobExecutionException(
        "Scheduled execution dispatcher is not configured yet for jobId=" + jobId);
  }
}
