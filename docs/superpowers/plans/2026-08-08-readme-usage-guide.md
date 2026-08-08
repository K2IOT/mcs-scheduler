# README Usage Guide Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `README.md` a self-contained, copy/paste-oriented guide for starting the scheduler, provisioning a Destination, creating/managing schedules, querying state, consuming Kafka results/events, using the Java client, and verifying the cluster.

**Architecture:** Keep `README.md` as the single developer entry point while treating controller classes, protobuf contracts, client interfaces, `application.yml`, Docker Compose, Flyway migrations, and CI as the authoritative sources. Detailed outage/recovery procedures remain in `docs/operations-runbook.md`; README links to them rather than duplicating them.

**Tech Stack:** Markdown, Spring Boot REST, Spring gRPC/protobuf, Apache Kafka CLI, PostgreSQL 16/psql, Docker Compose, Maven.

## Global Constraints

- Do not add or invent a public Destination API; use the supported database bootstrap path because current command surfaces require Destination to pre-exist.
- Every example must correspond to code on the current branch.
- Reuse one coherent namespace, destination ID, job ID, trigger IDs, and Kafka topic through the quick start.
- Use shell-generated future timestamps where one-shot examples would otherwise expire.
- Show `Idempotency-Key` on every REST command and request IDs on gRPC/Kafka commands.
- Show how to capture and reuse `ETag`/`If-Match` for mutations that require optimistic concurrency.
- Document only trigger types present in `RestModels.TriggerRequest` / protobuf contracts.
- Java examples must use methods present in `SchedulerClient` / `AsyncSchedulerClient`.
- Keep detailed operations procedures in `docs/operations-runbook.md`.

---

### Task 1: Audit documentation contracts and lock the runnable example vocabulary

**Files:**
- Read: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/RestModels.java`
- Read: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/JobController.java`
- Read: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/TriggerController.java`
- Read: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/ExecutionController.java`
- Read: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_command.proto`
- Read: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_query.proto`
- Read: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/common.proto`
- Read: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/SchedulerClient.java`
- Read: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/AsyncSchedulerClient.java`
- Read: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/SchedulerClientProperties.java`
- Read: `scheduler-service/src/main/resources/application.yml`
- Read: `docker/compose.yml`
- Read: `scheduler-service/src/main/resources/db/migration/V003__create_scheduler_tables.sql`
- Read: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes current public/controller/protobuf/client/config contracts.
- Produces the exact names, fields, topics, ports, SQL columns, and trigger variants used by Task 2.

- [ ] **Step 1: Confirm REST command coverage**

Verify the README cookbook covers these concrete routes:

```text
POST   /api/v1/schedules
POST   /api/v1/jobs
PUT    /api/v1/jobs/{jobId}
POST   /api/v1/jobs/{jobId}/pause
POST   /api/v1/jobs/{jobId}/resume
DELETE /api/v1/jobs/{jobId}
POST   /api/v1/triggers
PUT    /api/v1/triggers/{triggerId}
POST   /api/v1/triggers/{triggerId}/pause
POST   /api/v1/triggers/{triggerId}/resume
DELETE /api/v1/triggers/{triggerId}
POST   /api/v1/executions
```

- [ ] **Step 2: Confirm trigger recipes**

Document exactly these REST trigger discriminators from `RestModels`:

```text
ONCE
CRON
SIMPLE_INTERVAL
CALENDAR_INTERVAL
DAILY_TIME_INTERVAL
```

- [ ] **Step 3: Confirm query RPC coverage**

Document runnable `grpcurl` examples for:

```text
GetJob
ListJobTriggers
GetTrigger
GetExecution
ListExecutions
```

- [ ] **Step 4: Confirm Destination bootstrap schema**

Use the current Flyway schema columns and the Compose PostgreSQL container. The bootstrap example must insert a Kafka destination with:

```text
destination_id
version
namespace
type
topic
key_expression
headers
enabled
created_by
updated_by
```

- [ ] **Step 5: Confirm client/config/CI names**

Use exact property names from `SchedulerClientProperties` and `application.yml`, and exact CI job names from `.github/workflows/ci.yml`.

- [ ] **Step 6: Commit only if the audit requires a plan correction**

No source change is expected from this task; any discovered contract mismatch is corrected in this plan before Task 2.

---

### Task 2: Rewrite README into an executable developer journey

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes all audited names and examples from Task 1.
- Produces the repository's primary developer usage guide.

- [ ] **Step 1: Preserve and tighten architecture/concepts**

Keep the existing architecture diagram and explain the three first-class concepts:

```text
Destination = where an execution event is delivered
Job         = what event/payload is emitted
Trigger     = when the Job fires
```

State explicitly that `quartz.QRTZ_*` is a projection and must not be mutated directly.

- [ ] **Step 2: Add a five-minute Quick Start**

The quick start must be runnable in this order:

```bash
docker compose -f docker/compose.yml up --build -d
curl -fsS http://localhost:8081/actuator/health
curl -fsS http://localhost:8082/actuator/health
```

Then bootstrap one destination through `docker compose exec -T postgres psql ...`, generate a future UTC fire time, create a Job with two Triggers through REST, query it through gRPC, consume its destination event, and show safe teardown.

Use these stable IDs throughout:

```text
namespace: billing
destination: 44444444-4444-4444-4444-444444444444
job:         11111111-1111-1111-1111-111111111111
trigger 1:   33333333-3333-3333-3333-333333333333
trigger 2:   33333333-3333-3333-3333-333333333334
topic:       billing.invoice-events.v1
```

- [ ] **Step 3: Add Destination bootstrap section**

Show an idempotent-enough developer bootstrap using `INSERT ... ON CONFLICT (destination_id, version) DO UPDATE` against `scheduler.destination`. Explain that production destination lifecycle is an administrative/configuration concern and there is currently no public Destination command API.

- [ ] **Step 4: Add REST cookbook**

Provide copy/paste examples for create/update/pause/resume/delete Job, create/replace/pause/resume/delete Trigger, and manual fire. For revision-sensitive operations, demonstrate:

```bash
ETAG=$(curl -sS -D - ... | awk 'tolower($1)=="etag:" {gsub("\\r", "", $2); print $2}')
-H "If-Match: $ETAG"
```

Use the request bodies defined by `RestModels` and fresh `Idempotency-Key` values.

- [ ] **Step 5: Add gRPC cookbook**

Retain CreateSchedule and add real `grpcurl` calls for `GetJob`, `ListJobTriggers`, `GetTrigger`, `GetExecution`, and `ListExecutions`. Point readers to `scheduler-contracts/src/main/proto` for the exhaustive command/query contract.

- [ ] **Step 6: Add Kafka cookbook**

Show commands executed through the existing Kafka container, including record keys:

```bash
docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic mcs.scheduler.commands.v1 \
  --property parse.key=true --property key.separator='|'
```

Show consuming `mcs.scheduler.command-results.v1`, `mcs.scheduler.commands.v1.DLT`, and `billing.invoice-events.v1`. Explain request/message ID correlation and the `<namespace>:<aggregateId>` key convention.

- [ ] **Step 7: Add idempotency and concurrency behavior**

Document these four cases with concrete expected outcomes:

```text
same request ID + same payload    -> stored result, no duplicate definitions
same request ID + changed payload -> IDEMPOTENCY_CONFLICT
latest ETag in If-Match           -> mutation succeeds and revision advances
stale ETag in If-Match            -> optimistic-concurrency error
```

- [ ] **Step 8: Add all supported trigger recipes**

Provide minimal JSON `spec` bodies for:

```json
{"type":"ONCE","fireAt":"..."}
{"type":"CRON","expression":"0 0/5 * * * ?"}
{"type":"SIMPLE_INTERVAL","interval":"PT30S","repeatCount":10}
{"type":"CALENDAR_INTERVAL","interval":1,"unit":"DAYS"}
{"type":"DAILY_TIME_INTERVAL","interval":15,"unit":"MINUTES","daysOfWeek":["MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY"],"startTime":"09:00:00","endTime":"17:00:00"}
```

State that `timezone` is carried by the Trigger draft while the spec contains the cadence-specific fields.

- [ ] **Step 9: Expand Java client usage**

Show Maven dependency coordinates for `scheduler-client`, exact YAML property names, a synchronous `SchedulerClient.createSchedule(request, requestId)` snippet, and an asynchronous Kafka publication snippet using the actual `AsyncSchedulerClient` API audited in Task 1.

- [ ] **Step 10: Add configuration/observability/verification references**

Create a compact property table from `application.yml`, including datasource env vars from Compose, Kafka topics, outbox tuning, instance ID, REST/gRPC ports, Quartz cluster check-in and misfire threshold. Include:

```text
/actuator/health
/actuator/prometheus
```

Show a PostgreSQL query against `quartz.qrtz_scheduler_state` to verify both nodes and summarize the current CI gates from `.github/workflows/ci.yml`.

- [ ] **Step 11: Link the operations runbook without duplicating it**

Keep the README operations section short and link to `docs/operations-runbook.md` for outage, repair, reconciliation, rolling deploy, and failover procedures.

- [ ] **Step 12: Commit README**

```bash
git add README.md
git commit -m "docs: expand scheduler usage guide"
```

---

### Task 3: Verify README correctness and repository health

**Files:**
- Verify: `README.md`
- Verify: `docker/compose.yml`
- Verify: authoritative controller/proto/client/config files from Task 1

**Interfaces:**
- Consumes final README.
- Produces evidence that documentation examples match code and repository checks remain green.

- [ ] **Step 1: Perform source-to-doc contract review**

Check every REST path/header/body, gRPC service/method, Kafka topic/key, trigger discriminator, Destination SQL column, client method, and configuration property in README against the source files from Task 1.

- [ ] **Step 2: Run formatting/unit/package verification**

Run:

```bash
./mvnw -B -ntp -DskipITs spotless:check verify
```

Expected: PASS.

- [ ] **Step 3: Validate Compose**

Run:

```bash
docker compose -f docker/compose.yml config
```

Expected: exit 0 and valid rendered configuration.

- [ ] **Step 4: Review the branch diff**

Expected intended files only:

```text
README.md
docs/superpowers/specs/2026-08-08-readme-usage-guide-design.md
docs/superpowers/plans/2026-08-08-readme-usage-guide.md
```

- [ ] **Step 5: Open a draft pull request**

Title:

```text
docs: expand scheduler usage guide
```

PR body must summarize the end-to-end quick start, REST/gRPC/Kafka cookbooks, trigger/client/config coverage, and verification evidence.

- [ ] **Step 6: Verify CI for the final head**

Wait for the branch CI workflow associated with the PR and require the aggregate `validate` job to pass before claiming completion.

- [ ] **Step 7: Final commit only if verification corrections were needed**

Commit any documentation-only corrections with:

```bash
git add README.md docs/superpowers
 git commit -m "docs: correct scheduler usage examples"
```

## Plan self-review results

- Spec coverage: all eleven design sections map to Task 2 and all correctness/verification rules map to Tasks 1 and 3.
- Placeholder scan: no TBD/TODO/"similar to" implementation placeholders remain.
- Type/name consistency: controller routes, trigger discriminator names, gRPC query method names, fixed UUID vocabulary, topic names, and config sources are explicitly identified for audit before writing examples.
- Scope remains documentation-only; no scheduler behavior or public API is added.
