package io.k2iot.mcs.scheduler.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.command.SchedulerCommands;
import io.k2iot.mcs.scheduler.job.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.RecoveryPolicy;
import io.k2iot.mcs.scheduler.trigger.CronTriggerSpec;
import io.k2iot.mcs.scheduler.trigger.TriggerDefinition;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

class ScheduleControllerTest {

  private static final UUID REQUEST_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final UUID JOB_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
  private static final UUID TRIGGER_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");
  private static final UUID DESTINATION_ID =
      UUID.fromString("44444444-4444-4444-4444-444444444444");
  private static final Instant NOW = Instant.parse("2026-08-07T02:00:00Z");

  private SchedulerCommandFacade facade;
  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    facade = mock(SchedulerCommandFacade.class);
    RestCommandMapper mapper =
        new RestCommandMapper(JsonMapper.builder().findAndAddModules().build());
    mockMvc =
        MockMvcBuilders.standaloneSetup(new ScheduleController(facade, mapper))
            .setControllerAdvice(new ProblemDetailsAdvice())
            .build();
  }

  @Test
  void createsScheduleAndReturnsRevisionEtag() throws Exception {
    JobDefinition job = job();
    TriggerDefinition trigger = trigger();
    when(facade.createSchedule(any()))
        .thenReturn(new SchedulerCommands.ScheduleResult(job, List.of(trigger)));

    mockMvc
        .perform(
            post("/api/v1/schedules")
                .header("Idempotency-Key", REQUEST_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(validCreateScheduleJson("INV-1")))
        .andExpect(status().isCreated())
        .andExpect(header().string("ETag", "\"1\""))
        .andExpect(header().string("Location", "/api/v1/jobs/" + JOB_ID))
        .andExpect(jsonPath("$.job.jobId").value(JOB_ID.toString()))
        .andExpect(jsonPath("$.triggers.length()").value(1));
  }

  @Test
  void rejectsMissingIdempotencyKey() throws Exception {
    mockMvc
        .perform(
            post("/api/v1/schedules")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validCreateScheduleJson("INV-1")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("MISSING_IDEMPOTENCY_KEY"));

    verifyNoInteractions(facade);
  }

  @Test
  void rejectsPayloadLargerThan64Kib() throws Exception {
    String oversized = "x".repeat(65 * 1024);

    mockMvc
        .perform(
            post("/api/v1/schedules")
                .header("Idempotency-Key", REQUEST_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(validCreateScheduleJson(oversized)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("PAYLOAD_TOO_LARGE"));

    verifyNoInteractions(facade);
  }

  private static String validCreateScheduleJson(String invoiceId) {
    return """
        {
          "caller": "rest-test",
          "job": {
            "jobId": "%s",
            "namespace": "billing",
            "name": "invoice-dispatch",
            "description": "Dispatch invoice event",
            "destinationId": "%s",
            "destinationVersion": 1,
            "eventType": "invoice.due",
            "payload": {"invoiceId": "%s"},
            "headers": {},
            "concurrencyPolicy": "ALLOW",
            "recoveryPolicy": "NONE",
            "durable": true
          },
          "triggers": [
            {
              "triggerId": "%s",
              "jobId": "%s",
              "namespace": "billing",
              "name": "daily-invoice",
              "description": "Daily invoice dispatch",
              "spec": {
                "type": "CRON",
                "expression": "0 0 8 * * ?"
              },
              "startAt": null,
              "endAt": null,
              "priority": 5,
              "timezone": "Asia/Ho_Chi_Minh",
              "misfirePolicy": "DO_NOTHING",
              "calendarNames": []
            }
          ]
        }
        """
        .formatted(JOB_ID, DESTINATION_ID, invoiceId, TRIGGER_ID, JOB_ID);
  }

  private static JobDefinition job() {
    return new JobDefinition(
        JOB_ID,
        "billing",
        "invoice-dispatch",
        "Dispatch invoice event",
        DESTINATION_ID,
        1,
        "invoice.due",
        Map.of("invoiceId", "INV-1"),
        Map.of(),
        ConcurrencyPolicy.ALLOW,
        RecoveryPolicy.NONE,
        true,
        JobDefinition.State.ACTIVE,
        1,
        NOW,
        "rest-test",
        NOW,
        "rest-test");
  }

  private static TriggerDefinition trigger() {
    return new TriggerDefinition(
        TRIGGER_ID,
        JOB_ID,
        "billing",
        "daily-invoice",
        "Daily invoice dispatch",
        new CronTriggerSpec("0 0 8 * * ?"),
        null,
        null,
        5,
        "Asia/Ho_Chi_Minh",
        TriggerDefinition.MisfirePolicy.DO_NOTHING,
        Set.of(),
        TriggerDefinition.State.ACTIVE,
        1,
        NOW,
        "rest-test",
        NOW,
        "rest-test");
  }
}
