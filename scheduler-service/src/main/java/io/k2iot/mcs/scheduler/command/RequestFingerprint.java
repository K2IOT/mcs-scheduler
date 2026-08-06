package io.k2iot.mcs.scheduler.command;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public final class RequestFingerprint {

  private RequestFingerprint() {}

  public static String sha256(JsonMapper jsonMapper, Object value) {
    Objects.requireNonNull(jsonMapper, "jsonMapper");
    Objects.requireNonNull(value, "value");

    try {
      JsonNode canonical = canonicalize(jsonMapper.valueToTree(value), jsonMapper);
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(jsonMapper.writeValueAsBytes(canonical));
      return toHex(digest);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is not available", exception);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException("Command payload cannot be canonicalized", exception);
    }
  }

  private static JsonNode canonicalize(JsonNode node, JsonMapper jsonMapper) {
    if (node.isObject()) {
      ObjectNode canonical = jsonMapper.createObjectNode();
      List<Map.Entry<String, JsonNode>> fields = new ArrayList<>(node.properties());
      fields.sort(Comparator.comparing(Map.Entry::getKey));
      fields.forEach(
          field -> canonical.set(field.getKey(), canonicalize(field.getValue(), jsonMapper)));
      return canonical;
    }

    if (node.isArray()) {
      ArrayNode canonical = jsonMapper.createArrayNode();
      node.forEach(element -> canonical.add(canonicalize(element, jsonMapper)));
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
