package io.k2iot.mcs.scheduler.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class RequestFingerprint {

  private RequestFingerprint() {}

  public static String sha256(ObjectMapper objectMapper, Object value) {
    Objects.requireNonNull(objectMapper, "objectMapper");
    Objects.requireNonNull(value, "value");

    JsonNode canonical = canonicalize(objectMapper.valueToTree(value), objectMapper);
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(objectMapper.writeValueAsBytes(canonical));
      return toHex(digest);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is not available", exception);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Command payload cannot be canonicalized", exception);
    }
  }

  private static JsonNode canonicalize(JsonNode node, ObjectMapper objectMapper) {
    if (node.isObject()) {
      ObjectNode canonical = objectMapper.createObjectNode();
      List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
      node.fields().forEachRemaining(fields::add);
      fields.sort(Comparator.comparing(Map.Entry::getKey));
      fields.forEach(
          field -> canonical.set(field.getKey(), canonicalize(field.getValue(), objectMapper)));
      return canonical;
    }

    if (node.isArray()) {
      ArrayNode canonical = objectMapper.createArrayNode();
      node.forEach(element -> canonical.add(canonicalize(element, objectMapper)));
      return canonical;
    }

    return node;
  }

  private static String toHex(byte[] bytes) {
    StringBuilder result = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      result.append(Character.forDigit((value >>> 4) & 0x0f, 16));
      result.append(Character.forDigit(value & 0x0f, 16));
    }
    return result.toString();
  }
}
