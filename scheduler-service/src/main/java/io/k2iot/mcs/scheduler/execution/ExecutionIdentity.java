package io.k2iot.mcs.scheduler.execution;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public final class ExecutionIdentity {

  private ExecutionIdentity() {}

  public static UUID forScheduled(UUID triggerId, Instant scheduledFireTime) {
    Objects.requireNonNull(triggerId, "triggerId");
    Objects.requireNonNull(scheduledFireTime, "scheduledFireTime");

    String source = "scheduled:" + triggerId + ":" + scheduledFireTime.toEpochMilli();
    byte[] digest = sha256(source.getBytes(StandardCharsets.UTF_8));
    byte[] uuidBytes = Arrays.copyOf(digest, 16);

    uuidBytes[6] = (byte) ((uuidBytes[6] & 0x0f) | 0x50);
    uuidBytes[8] = (byte) ((uuidBytes[8] & 0x3f) | 0x80);

    ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
    return new UUID(buffer.getLong(), buffer.getLong());
  }

  public static UUID forManual(UUID manualFireId) {
    return Objects.requireNonNull(manualFireId, "manualFireId");
  }

  private static byte[] sha256(byte[] source) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(source);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is not available", exception);
    }
  }
}
