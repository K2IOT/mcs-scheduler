package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.testing.PostgresIntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class PersistenceSchemaIT extends PostgresIntegrationTestBase {

  @Autowired JdbcTemplate jdbc;

  @Test
  void exposesColumnsRequiredByPersistenceAdaptersAndIdempotency() {
    assertThat(columnExists("scheduler", "job_definition", "durable")).isTrue();
    assertThat(columnExists("scheduler", "command_request", "request_hash")).isTrue();
    assertThat(columnExists("scheduler", "command_request", "response_json")).isTrue();
  }

  private boolean columnExists(String schema, String table, String column) {
    Integer count =
        jdbc.queryForObject(
            """
            select count(*)
            from information_schema.columns
            where table_schema = ? and table_name = ? and column_name = ?
            """,
            Integer.class,
            schema,
            table,
            column);
    return count != null && count == 1;
  }
}
