# Task 9 and Task 10 Critical Test Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add real integration coverage for durable execution idempotency and bounded Kafka retry semantics.

**Architecture:** Keep production behavior unchanged. Add a dedicated PostgreSQL integration test for execution identity/outbox deduplication and extend the existing Kafka integration test with a test-only flaky `SchedulerCommandFacade` that delegates to the real facade after controlled transient failures.

**Tech Stack:** Java 21, Spring Boot 4.0.7, Spring Kafka, Quartz, PostgreSQL Testcontainers, Embedded Kafka, JUnit 5, AssertJ, Mockito.

## Global Constraints

- Do not change production code unless a new regression test exposes a real defect.
- Use real PostgreSQL repositories for execution idempotency assertions.
- Use `mcs.scheduler.kafka.retry-attempts=2` and a short test backoff for bounded retry verification.
- Keep transient failure injection in test code only.
- Keep DLT reads deterministic by matching the expected scheduler message ID.

---

### Task 1: PostgreSQL-backed durable execution idempotency

**Files:**
- Create: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/execution/ScheduledExecutionIdempotencyIT.java`

**Interfaces:**
- Consumes: `ScheduledExecutionService.record(JobExecutionContext)` and real scheduler JDBC repositories.
- Produces: regression coverage for scheduled, recovery, and manual logical execution identity.

- [ ] **Step 1: Add integration fixture and data setup**

Create a `PostgresIntegrationTestBase` test class that inserts one enabled destination, one active job, and one active trigger before each test and clears execution/outbox/domain rows in foreign-key-safe order.

- [ ] **Step 2: Verify repeated scheduled fire idempotency**

Call `record()` twice with the same trigger ID and scheduled fire time. Assert `scheduler.execution` contains exactly one row for `ExecutionIdentity.forScheduled(triggerId, fireTime)` and `scheduler.outbox_event` contains exactly one row with that aggregate ID.

- [ ] **Step 3: Verify Quartz recovery idempotency**

Call `record()` once with the normal context and once with a recovering context whose merged `JobDataMap` omits `triggerId` and whose `getRecoveringTriggerKey()` contains the scheduler trigger UUID. Assert exactly one execution and one outbox event for the original logical execution ID.

- [ ] **Step 4: Verify repeated manual fire idempotency**

Call `record()` twice with the same `manualFireId`. Assert one execution row with that `execution_id`/`manual_fire_id`, null scheduled trigger identity columns as required by the schema, and one outbox event.

- [ ] **Step 5: Run focused verification**

Run:

```bash
./mvnw -B -ntp -pl scheduler-service -am -Dit.test=ScheduledExecutionIdempotencyIT verify
```

Expected: PASS without production-code changes. If it fails because production behavior is incorrect, switch to `superpowers:systematic-debugging` before changing production code.

---

### Task 2: Kafka bounded retry integration coverage

**Files:**
- Modify: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/kafka/SchedulerCommandKafkaIT.java`

**Interfaces:**
- Consumes: production `KafkaTopicConfiguration.schedulerKafkaErrorHandler`, `SchedulerCommandListener`, and `SchedulerCommandFacade` behavior.
- Produces: retry-success and retry-exhaustion coverage with transactional side-effect assertions.

- [ ] **Step 1: Configure deterministic retry timing**

Add test properties:

```text
mcs.scheduler.kafka.retry-backoff-ms=10
mcs.scheduler.kafka.retry-attempts=2
```

- [ ] **Step 2: Add test-only flaky facade**

Import a nested `@TestConfiguration` that defines a `FlakySchedulerCommandFacade` bean. The subclass increments an attempt counter for `createSchedule`, throws a plain retriable `IllegalStateException` while configured failures remain, and otherwise delegates to `super.createSchedule(command)`. Expose reset/fail-next/attempt-count helpers to the test class.

- [ ] **Step 3: Verify retry then success**

Configure two failures, publish a valid unique `CREATE_SCHEDULE` envelope, wait for the command-result outbox row, and assert exactly three facade attempts, one inbox row, one job, one trigger, one result outbox event, and no DLT record carrying that message ID.

- [ ] **Step 4: Verify retry exhaustion**

Configure at least three failures, consume the DLT topic, publish a valid unique envelope, await the DLT record matching that message ID, and assert exactly three attempts. Assert zero inbox, command-request, job, trigger, and outbox rows for the failed command and verify stable message ID, request ID, error code, original topic, partition, and offset headers.

- [ ] **Step 5: Make DLT reads test-order safe**

Replace single-record assumptions with an `awaitDlt` helper that polls until the expected scheduler message ID is found or a bounded timeout expires.

- [ ] **Step 6: Run focused verification**

Run:

```bash
./mvnw -B -ntp -pl scheduler-service -am -Dit.test=SchedulerCommandKafkaIT verify
```

Expected: PASS. If the retry count or transactional rollback semantics differ from the configured contract, use `superpowers:systematic-debugging` before changing production code.

---

### Task 3: Full verification

**Files:**
- Verify all changed test and documentation files.

- [ ] **Step 1: Run formatting check**

```bash
./mvnw -B -ntp spotless:check
```

Expected: PASS.

- [ ] **Step 2: Run full repository verification**

```bash
./mvnw -B -ntp verify
```

Expected: PASS with all unit and integration tests green.

- [ ] **Step 3: Review branch diff**

Confirm only the approved docs/tests changed unless a production bug required a separately justified fix.