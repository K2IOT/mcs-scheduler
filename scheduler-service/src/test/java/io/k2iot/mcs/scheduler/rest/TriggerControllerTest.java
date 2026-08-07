package io.k2iot.mcs.scheduler.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.command.SchedulerCommandFacade;
import io.k2iot.mcs.scheduler.trigger.InvalidTriggerException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

class TriggerControllerTest {

  private static final UUID REQUEST_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
  private static final UUID JOB_ID = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
  private static final UUID TRIGGER_ID = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");

  private SchedulerCommandFacade facade;
  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    facade = mock(SchedulerCommandFacade.class);
    RestCommandMapper mapper =
        new RestCommandMapper(JsonMapper.builder().findAndAddModules().build());
    mockMvc =
        MockMvcBuilders.standaloneSetup(new TriggerController(facade, mapper))
            .setControllerAdvice(new ProblemDetailsAdvice())
            .build();
  }

  @Test
  void mapsStaleRevisionToPreconditionFailed() throws Exception {
    when(facade.replaceTrigger(any()))
        .thenThrow(
            new SchedulerCommandException("REVISION_CONFLICT", "Expected revision 7 but found 8"));

    mockMvc
        .perform(
            put("/api/v1/triggers/{triggerId}", TRIGGER_ID)
                .header("Idempotency-Key", REQUEST_ID)
                .header("If-Match", "\"7\"")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validReplaceTriggerJson("0 0 8 * * ?")))
        .andExpect(status().isPreconditionFailed())
        .andExpect(jsonPath("$.code").value("REVISION_CONFLICT"));
  }

  @Test
  void mapsInvalidCronToBadRequestWithStableCode() throws Exception {
    when(facade.replaceTrigger(any()))
        .thenThrow(new InvalidTriggerException("INVALID_CRON", "Invalid cron expression"));

    mockMvc
        .perform(
            put("/api/v1/triggers/{triggerId}", TRIGGER_ID)
                .header("Idempotency-Key", REQUEST_ID)
                .header("If-Match", "\"7\"")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validReplaceTriggerJson("not-a-cron")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_CRON"));
  }

  @Test
  void rejectsBodyAndPathTriggerIdMismatch() throws Exception {
    UUID otherTriggerId = UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd");

    mockMvc
        .perform(
            put("/api/v1/triggers/{triggerId}", otherTriggerId)
                .header("Idempotency-Key", REQUEST_ID)
                .header("If-Match", "\"7\"")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validReplaceTriggerJson("0 0 8 * * ?")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("RESOURCE_ID_MISMATCH"));
  }

  private static String validReplaceTriggerJson(String expression) {
    return """
        {
          "caller": "rest-test",
          "trigger": {
            "triggerId": "%s",
            "jobId": "%s",
            "namespace": "billing",
            "name": "daily-invoice",
            "description": "Daily invoice dispatch",
            "spec": {
              "type": "CRON",
              "expression": "%s"
            },
            "startAt": null,
            "endAt": null,
            "priority": 5,
            "timezone": "Asia/Ho_Chi_Minh",
            "misfirePolicy": "DO_NOTHING",
            "calendarNames": []
          }
        }
        """
        .formatted(TRIGGER_ID, JOB_ID, expression);
  }
}
