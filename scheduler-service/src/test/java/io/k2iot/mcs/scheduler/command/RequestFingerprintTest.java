package io.k2iot.mcs.scheduler.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RequestFingerprintTest {

  private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

  @Test
  void hashesCanonicalJsonIndependentlyOfObjectPropertyOrder() {
    Map<String, Object> first = new LinkedHashMap<>();
    first.put("namespace", "billing");
    first.put("payload", Map.of("amount", 1250, "currency", "VND"));
    first.put("headers", List.of("trace-id", "tenant-id"));

    Map<String, Object> second = new LinkedHashMap<>();
    second.put("headers", List.of("trace-id", "tenant-id"));
    second.put("payload", Map.of("currency", "VND", "amount", 1250));
    second.put("namespace", "billing");

    assertThat(RequestFingerprint.sha256(objectMapper, first))
        .isEqualTo(RequestFingerprint.sha256(objectMapper, second))
        .hasSize(64);
  }

  @Test
  void preservesArrayOrderWhenCanonicalizing() {
    String first = RequestFingerprint.sha256(objectMapper, Map.of("values", List.of(1, 2, 3)));
    String second = RequestFingerprint.sha256(objectMapper, Map.of("values", List.of(3, 2, 1)));

    assertThat(first).isNotEqualTo(second);
  }
}
