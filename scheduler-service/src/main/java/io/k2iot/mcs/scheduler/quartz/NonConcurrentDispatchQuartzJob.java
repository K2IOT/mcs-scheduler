package io.k2iot.mcs.scheduler.quartz;

import org.quartz.DisallowConcurrentExecution;

@DisallowConcurrentExecution
public final class NonConcurrentDispatchQuartzJob extends ConcurrentDispatchQuartzJob {}
