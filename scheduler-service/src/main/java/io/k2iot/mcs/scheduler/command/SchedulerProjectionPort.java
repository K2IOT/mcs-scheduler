package io.k2iot.mcs.scheduler.command;

import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.util.UUID;

public interface SchedulerProjectionPort {

  void createJob(JobDefinition definition);

  void updateJob(JobDefinition definition);

  void pauseJob(JobDefinition definition);

  void resumeJob(JobDefinition definition);

  void deleteJob(JobDefinition definition);

  void createTrigger(TriggerDefinition definition);

  void replaceTrigger(TriggerDefinition definition);

  void pauseTrigger(TriggerDefinition definition);

  void resumeTrigger(TriggerDefinition definition);

  void deleteTrigger(TriggerDefinition definition);

  void fireTriggerNow(TriggerDefinition definition, UUID manualFireId);
}
