package io.k2iot.mcs.scheduler.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ExecutionIdentityTest {

  private static final UUID TRIGGER_ID = UUID.fromString("12345678-1234-5678-9abc-def012345678");
  private static final Instant FIRE_TIME = Instant.parse("2027-12-15T01:00:00Z");

  @Test
  void scheduledIdentityIsStableAcrossRecoveryAttempts() {
    UUID first = ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME);
    UUID second = ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME);

    assertThat(second).isEqualTo(first);
  }

  @Test
  void scheduledIdentityMatchesTheDocumentedSha256Derivation() {
    assertThat(ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME))
        .isEqualTo(UUID.fromString("d1ec868c-a861-59de-9f0d-d7193a44a816"));
  }

  @Test
  void scheduledIdentityUsesRfc4122VariantAndVersionFiveBits() {
    UUID identity = ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME);

    assertThat(identity.version()).isEqualTo(5);
    assertThat(identity.variant()).isEqualTo(2);
  }

  @Test
  void scheduledIdentityChangesWithEitherStableInput() {
    assertThat(ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME.plusMillis(1)))
        .isNotEqualTo(ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME));
    assertThat(
            ExecutionIdentity.forScheduled(
                UUID.fromString("87654321-4321-6789-abcd-ef0123456789"), FIRE_TIME))
        .isNotEqualTo(ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME));
  }

  @Test
  void manualIdentityReturnsTheCallerSuppliedId() {
    UUID manualFireId = UUID.fromString("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");

    assertThat(ExecutionIdentity.forManual(manualFireId)).isSameAs(manualFireId);
  }

  @Test
  void identityInputsAreRequired() {
    assertThatNullPointerException()
        .isThrownBy(() -> ExecutionIdentity.forScheduled(null, FIRE_TIME));
    assertThatNullPointerException()
        .isThrownBy(() -> ExecutionIdentity.forScheduled(TRIGGER_ID, null));
    assertThatNullPointerException().isThrownBy(() -> ExecutionIdentity.forManual(null));
  }
}
