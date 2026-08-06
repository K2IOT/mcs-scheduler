package io.k2iot.mcs.scheduler.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import java.util.Set;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class DatabaseMigrationIT extends PostgresIntegrationTestBase {

  private static final Set<String> DOMAIN_TABLES =
      Set.of(
          "destination",
          "job_definition",
          "trigger_definition",
          "command_request",
          "inbox_message",
          "execution",
          "outbox_event",
          "audit_event");

  private static final Set<String> QUARTZ_TABLES =
      Set.of(
          "qrtz_job_details",
          "qrtz_triggers",
          "qrtz_simple_triggers",
          "qrtz_cron_triggers",
          "qrtz_simprop_triggers",
          "qrtz_blob_triggers",
          "qrtz_calendars",
          "qrtz_paused_trigger_grps",
          "qrtz_fired_triggers",
          "qrtz_scheduler_state",
          "qrtz_locks");

  @Autowired JdbcTemplate jdbc;

  @Autowired Flyway flyway;

  @Test
  void createsDomainAndQuartzTables() {
    assertThat(tableNames("scheduler")).containsExactlyInAnyOrderElementsOf(DOMAIN_TABLES);
    assertThat(tableNames("quartz")).containsExactlyInAnyOrderElementsOf(QUARTZ_TABLES);
  }

  @Test
  void usesJsonbForPayloadsAndTimestampWithTimeZoneForInstants() {
    assertThat(columnType("scheduler", "job_definition", "payload")).isEqualTo("jsonb");
    assertThat(columnType("scheduler", "trigger_definition", "spec")).isEqualTo("jsonb");
    assertThat(columnType("scheduler", "execution", "payload")).isEqualTo("jsonb");
    assertThat(columnType("scheduler", "outbox_event", "payload")).isEqualTo("jsonb");

    assertThat(columnType("scheduler", "job_definition", "created_at"))
        .isEqualTo("timestamp with time zone");
    assertThat(columnType("scheduler", "trigger_definition", "start_at"))
        .isEqualTo("timestamp with time zone");
    assertThat(columnType("scheduler", "execution", "scheduled_fire_time"))
        .isEqualTo("timestamp with time zone");
  }

  @Test
  void createsRequiredUniqueConstraintsAndIndexes() {
    assertThat(constraintNames("scheduler", "command_request"))
        .contains("uq_command_request_request_id");
    assertThat(constraintNames("scheduler", "inbox_message"))
        .contains("uq_inbox_message_message_id");

    assertThat(indexNames("scheduler"))
        .contains(
            "ux_job_definition_namespace_name_active",
            "ux_job_definition_id_name_active",
            "ux_execution_scheduled_fire",
            "ux_execution_manual_fire");

    assertThat(indexNames("quartz"))
        .contains(
            "idx_qrtz_j_req_recovery",
            "idx_qrtz_t_nft_st_misfire",
            "idx_qrtz_ft_inst_job_req_rcvry");
  }

  @Test
  void runningFlywayAgainIsIdempotent() {
    assertThatCode(() -> flyway.migrate()).doesNotThrowAnyException();
    assertThat(flyway.info().pending()).isEmpty();
  }

  private Set<String> tableNames(String schema) {
    return Set.copyOf(
        jdbc.queryForList(
            "select table_name from information_schema.tables where table_schema = ? and table_type = 'BASE TABLE'",
            String.class,
            schema));
  }

  private String columnType(String schema, String table, String column) {
    return jdbc.queryForObject(
        "select data_type from information_schema.columns where table_schema = ? and table_name = ? and column_name = ?",
        String.class,
        schema,
        table,
        column);
  }

  private Set<String> constraintNames(String schema, String table) {
    return Set.copyOf(
        jdbc.queryForList(
            "select constraint_name from information_schema.table_constraints where table_schema = ? and table_name = ?",
            String.class,
            schema,
            table));
  }

  private Set<String> indexNames(String schema) {
    return Set.copyOf(
        jdbc.queryForList(
            "select indexname from pg_indexes where schemaname = ?", String.class, schema));
  }
}
