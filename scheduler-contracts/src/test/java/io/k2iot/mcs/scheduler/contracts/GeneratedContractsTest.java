package io.k2iot.mcs.scheduler.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.grpc.ServiceDescriptor;
import io.k2iot.mcs.scheduler.v1.CronTrigger;
import io.k2iot.mcs.scheduler.v1.JobDefinition;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.ScheduledExecutionEvent;
import io.k2iot.mcs.scheduler.v1.SchedulerCommandServiceGrpc;
import io.k2iot.mcs.scheduler.v1.SchedulerQueryServiceGrpc;
import io.k2iot.mcs.scheduler.v1.TriggerDefinition;
import io.k2iot.mcs.scheduler.v1.TriggerSpec;
import java.util.List;
import org.junit.jupiter.api.Test;

class GeneratedContractsTest {

  @Test
  void cronTriggerRoundTripsThroughProtobuf() throws Exception {
    TriggerSpec source =
        TriggerSpec.newBuilder()
            .setCron(
                CronTrigger.newBuilder()
                    .setExpression("0 0 8 * * ?")
                    .setTimezone("Asia/Ho_Chi_Minh"))
            .build();

    assertThat(TriggerSpec.parseFrom(source.toByteArray())).isEqualTo(source);
  }

  @Test
  void commandServiceExposesAllPlannedMutations() {
    assertThat(rpcNames(SchedulerCommandServiceGrpc.getServiceDescriptor()))
        .containsExactly(
            "CreateJob",
            "UpdateJob",
            "PauseJob",
            "ResumeJob",
            "DeleteJob",
            "CreateTrigger",
            "ReplaceTrigger",
            "PauseTrigger",
            "ResumeTrigger",
            "DeleteTrigger",
            "FireTriggerNow",
            "CreateSchedule");
  }

  @Test
  void queryServiceExposesResourceAndExecutionLookups() {
    assertThat(rpcNames(SchedulerQueryServiceGrpc.getServiceDescriptor()))
        .containsExactly(
            "GetJob", "ListJobTriggers", "GetTrigger", "GetExecution", "ListExecutions");
  }

  @Test
  void triggerOneofKeepsStableFieldNumbers() {
    assertThat(TriggerSpec.getDescriptor().findFieldByName("once").getNumber()).isEqualTo(1);
    assertThat(TriggerSpec.getDescriptor().findFieldByName("cron").getNumber()).isEqualTo(2);
    assertThat(TriggerSpec.getDescriptor().findFieldByName("simple_interval").getNumber())
        .isEqualTo(3);
    assertThat(TriggerSpec.getDescriptor().findFieldByName("calendar_interval").getNumber())
        .isEqualTo(4);
    assertThat(TriggerSpec.getDescriptor().findFieldByName("daily_time_interval").getNumber())
        .isEqualTo(5);
  }

  @Test
  void scheduleResponseRepresentsOneJobWithMultipleTriggers() {
    ScheduleResponse response =
        ScheduleResponse.newBuilder()
            .setJob(JobDefinition.newBuilder().setJobId("job-1").setNamespace("billing"))
            .addTriggers(TriggerDefinition.newBuilder().setTriggerId("trigger-1").setJobId("job-1"))
            .addTriggers(TriggerDefinition.newBuilder().setTriggerId("trigger-2").setJobId("job-1"))
            .build();

    assertThat(response.getJob().getNamespace()).isEqualTo("billing");
    assertThat(response.getTriggersList())
        .extracting(TriggerDefinition::getTriggerId)
        .containsExactly("trigger-1", "trigger-2");
  }

  @Test
  void scheduledExecutionEventRoundTripsStructuredPayload() throws Exception {
    Struct payload =
        Struct.newBuilder()
            .putFields("orderId", Value.newBuilder().setStringValue("order-42").build())
            .build();
    ScheduledExecutionEvent source =
        ScheduledExecutionEvent.newBuilder()
            .setSchemaVersion(1)
            .setExecutionId("execution-1")
            .setNamespace("billing")
            .setEventType("invoice.due")
            .setJobId("job-1")
            .setTriggerId("trigger-1")
            .setScheduledFireTime(Timestamp.newBuilder().setSeconds(1_800_000_000L))
            .setActualFireTime(Timestamp.newBuilder().setSeconds(1_800_000_001L))
            .setPayload(payload)
            .putHeaders("traceparent", "00-test")
            .build();

    assertThat(ScheduledExecutionEvent.parseFrom(source.toByteArray())).isEqualTo(source);
  }

  private static List<String> rpcNames(ServiceDescriptor service) {
    return service.getMethods().stream()
        .map(method -> method.getFullMethodName())
        .map(name -> name.substring(name.lastIndexOf('/') + 1))
        .toList();
  }
}
