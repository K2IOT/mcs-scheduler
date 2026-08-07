package io.k2iot.mcs.scheduler.quartz;

import java.util.Objects;
import java.util.UUID;
import org.quartz.JobKey;
import org.quartz.TriggerKey;

public final class QuartzKeys {

  public static final String JOB_ID = "jobId";
  public static final String TRIGGER_ID = "triggerId";
  public static final String MANUAL_FIRE_ID = "manualFireId";
  public static final String NAMESPACE = "namespace";
  public static final String REVISION = "revision";

  private QuartzKeys() {}

  public static JobKey job(UUID jobId, String namespace) {
    Objects.requireNonNull(jobId, "jobId");
    return JobKey.jobKey(jobId.toString(), requireNamespace(namespace));
  }

  public static TriggerKey trigger(UUID triggerId, String namespace) {
    Objects.requireNonNull(triggerId, "triggerId");
    return TriggerKey.triggerKey(triggerId.toString(), requireNamespace(namespace));
  }

  private static String requireNamespace(String namespace) {
    Objects.requireNonNull(namespace, "namespace");
    if (namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be blank");
    }
    return namespace;
  }
}
