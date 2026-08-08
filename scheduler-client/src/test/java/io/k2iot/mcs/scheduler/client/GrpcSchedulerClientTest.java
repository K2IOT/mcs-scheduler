package io.k2iot.mcs.scheduler.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.SchedulerCommandServiceGrpc;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GrpcSchedulerClientTest {

  private static final UUID GENERATED_REQUEST_ID =
      UUID.fromString("71000000-0000-4000-8000-000000000001");
  private static final UUID SUPPLIED_REQUEST_ID =
      UUID.fromString("72000000-0000-4000-8000-000000000001");

  @Test
  void preservesSuppliedRequestId() {
    var stub = mock(SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub.class);
    when(stub.createSchedule(any())).thenReturn(ScheduleResponse.getDefaultInstance());
    Supplier<UUID> requestIds = () -> GENERATED_REQUEST_ID;
    var client = new GrpcSchedulerClient(stub, requestIds);

    client.createSchedule(baseRequest(), SUPPLIED_REQUEST_ID);

    ArgumentCaptor<CreateScheduleRequest> request =
        ArgumentCaptor.forClass(CreateScheduleRequest.class);
    verify(stub).createSchedule(request.capture());
    assertThat(request.getValue().getRequestId()).isEqualTo(SUPPLIED_REQUEST_ID.toString());
  }

  @Test
  void generatesMissingRequestIdOncePerLogicalCall() {
    var stub = mock(SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub.class);
    when(stub.createSchedule(any())).thenReturn(ScheduleResponse.getDefaultInstance());
    var calls = new int[1];
    Supplier<UUID> requestIds =
        () -> {
          calls[0]++;
          return GENERATED_REQUEST_ID;
        };
    var client = new GrpcSchedulerClient(stub, requestIds);

    client.createSchedule(baseRequest(), null);

    ArgumentCaptor<CreateScheduleRequest> request =
        ArgumentCaptor.forClass(CreateScheduleRequest.class);
    verify(stub).createSchedule(request.capture());
    assertThat(calls[0]).isEqualTo(1);
    assertThat(request.getValue().getRequestId()).isEqualTo(GENERATED_REQUEST_ID.toString());
  }

  @Test
  void rejectsCascadeDeleteUnsupportedBySchedulerV1() {
    var stub = mock(SchedulerCommandServiceGrpc.SchedulerCommandServiceBlockingStub.class);
    var client = new GrpcSchedulerClient(stub, () -> GENERATED_REQUEST_ID);

    assertThatThrownBy(
            () ->
                client.deleteJob(
                    UUID.fromString("77000000-0000-4000-8000-000000000001"),
                    "billing",
                    1,
                    true,
                    SUPPLIED_REQUEST_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cascade");
    verifyNoInteractions(stub);
  }

  private CreateScheduleRequest baseRequest() {
    return CreateScheduleRequest.newBuilder()
        .setNamespace("billing")
        .setCaller("billing-service")
        .build();
  }
}
