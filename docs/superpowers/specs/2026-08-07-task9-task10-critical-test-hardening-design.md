# Task 9 and Task 10 Critical Test Hardening Design

## Goal

Close the two critical verification gaps identified after Task 10 without changing scheduler production behavior unless a regression test exposes a real defect.

## Scope

### Durable execution idempotency

Add PostgreSQL-backed integration coverage for `ScheduledExecutionService` using the real `JdbcExecutionRepository` and real outbox repository. Verify that the same logical scheduled firing, Quartz recovery of that firing, and the same manual fire ID each persist exactly one `scheduler.execution` row and exactly one execution outbox event.

The recovery case must exercise Quartz recovery identity resolution from `getRecoveringTriggerKey()` when the merged `JobDataMap` does not contain `triggerId`.

### Kafka bounded retry

Extend `SchedulerCommandKafkaIT` to configure `retry-attempts=2` with a short test backoff and inject a test-only `SchedulerCommandFacade` subclass that can fail a controlled number of `CREATE_SCHEDULE` attempts before delegating to the real facade.

Verify two behaviors:

1. Two transient failures are retried and the third attempt succeeds, producing one inbox row, one job, one trigger, one command-result outbox event, and no DLT record for that message.
2. Three transient failures exhaust the configured initial attempt plus two retries, roll back the listener transaction on every attempt, leave no inbox/domain/outbox side effects, and publish exactly one DLT record with stable message/request/error metadata.

DLT assertions must filter by message ID so the embedded Kafka topic remains deterministic across test orderings.

## Boundaries

- Production classes and configuration remain unchanged if the new tests pass.
- Test-only failure injection stays under `src/test` and delegates to the real command facade on successful attempts.
- Existing atomic execution/outbox rollback coverage remains separate and unchanged.
- Existing Kafka duplicate and non-retryable DLT coverage remains intact.

## Verification

Run focused integration verification for the two test classes, then repository `./mvnw -B -ntp verify` and `./mvnw -B -ntp spotless:check` through GitHub CI on the branch/PR.