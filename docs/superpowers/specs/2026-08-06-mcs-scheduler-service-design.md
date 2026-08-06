# MCS Scheduler Service Design

**Date:** 2026-08-06  
**Status:** Proposed baseline for implementation planning  
**Repository:** `K2IOT/mcs-scheduler`

## 1. Purpose

Build a reusable scheduling control-plane service for a microservice platform. Other services can create and manage long-lived schedules through REST, gRPC, or Kafka commands. Quartz executes persisted triggers in a PostgreSQL-backed cluster, while business execution is delivered asynchronously through Kafka.

The service must preserve Quartz's native separation between a **Job** and a **Trigger**:

- A **Job** defines what business message will be emitted and which registered destination owns it.
- A **Trigger** defines when that job fires.
- One job may have zero, one, or many triggers.
- A convenience Schedule API may create a job and one or more triggers in one transaction, but it does not hide or replace the underlying Job/Trigger model.

## 2. Goals

1. Persist schedules for days, months, and years across restarts and deployments.
2. Run two or more scheduler nodes against the same PostgreSQL Quartz store with cluster failover and load balancing.
3. Support dynamic creation, update, pause, resume, deletion, inspection, and manual firing.
4. Expose equivalent capabilities through REST, gRPC, and Kafka command ingestion.
5. Deliver scheduled business work durably through Kafka without executing arbitrary consumer business code inside Quartz threads.
6. Make every mutation and firing idempotent and observable.
7. Provide unit, contract, integration, cluster, and recovery tests using mocks and Testcontainers.
8. Publish a small contracts/client module so other Java microservices can integrate without depending on scheduler internals.

## 3. Non-goals for the first implementation

1. The scheduler is not a general workflow engine. Multi-step durable workflows, human approval, compensation, and long-running state machines belong in a workflow engine such as Temporal.
2. Consumers cannot submit arbitrary Java class names or executable code as Quartz jobs.
3. Quartz is not used as a high-throughput work queue. Quartz only determines when a durable business event becomes due.
4. The first delivery adapter is Kafka. HTTP and outbound gRPC callbacks are extension points, not initial production transports.
5. The service does not provide a visual administration UI in the first release.

## 4. Technology baseline

- Java 21.
- Maven multi-module build.
- Spring Boot 4.0.x line.
- Spring Framework managed Quartz integration with Quartz 2.5.x.
- Spring gRPC 1.0.x.
- PostgreSQL 16 or later.
- Flyway for database migrations.
- Spring for Apache Kafka.
- Jackson JSON for REST and stored business payloads.
- Protocol Buffers for gRPC contracts.
- JUnit 5, AssertJ, Mockito, Spring Boot Test, Spring gRPC test support, and Testcontainers.
- Micrometer and Spring Boot Actuator.

Dependency versions should be controlled by the Spring Boot and Spring gRPC BOMs. Quartz must remain on the Jakarta-compatible 2.5.x line. Do not override transitive versions unless a documented compatibility or security reason requires it.

## 5. Architectural decision

### 5.1 Control plane versus execution plane

The service is split logically into two planes:

- **Control plane:** accepts commands, validates definitions, persists domain metadata, and creates or changes Quartz JobDetails and Triggers.
- **Execution plane:** Quartz acquires due triggers, invokes one generic dispatcher job, records the scheduled execution, and writes an outbox event for Kafka delivery.

Business microservices consume the emitted event and execute their own business logic. Scheduler nodes never load consumer service classes and never make a business transaction span scheduler and consumer databases.

### 5.2 Why Kafka delivery is separated from Quartz execution

Calling a target service directly from a Quartz job creates ambiguous failure cases. A node can complete the remote call and crash before Quartz records completion, causing recovery to call the service again. The design instead performs a short local PostgreSQL transaction:

1. Derive a deterministic execution identity.
2. Insert the execution record if it does not already exist.
3. Insert an outbox record in the same transaction.
4. Return from the Quartz job.
5. Publish the outbox record to Kafka independently.

A unique database constraint makes repeated Quartz recovery attempts converge on one logical execution. Consumers still use the execution identifier as their idempotency key.

## 6. Repository/module structure

```text
mcs-scheduler/
├── pom.xml
├── scheduler-contracts/
│   ├── pom.xml
│   └── src/main/proto/mcs/scheduler/v1/scheduler.proto
├── scheduler-client/
│   ├── pom.xml
│   └── src/main/java/io/k2iot/mcs/scheduler/client/...
├── scheduler-service/
│   ├── pom.xml
│   ├── src/main/java/io/k2iot/mcs/scheduler/...
│   ├── src/main/resources/application.yml
│   └── src/main/resources/db/migration/...
├── docker/
│   └── compose.yml
└── docs/
```

The service module uses package-by-feature boundaries rather than a global controller/service/repository layout:

```text
io.k2iot.mcs.scheduler
├── job/
├── trigger/
├── execution/
├── destination/
├── command/
├── quartz/
├── outbox/
├── rest/
├── grpc/
├── kafka/
├── observability/
└── configuration/
```

Each feature contains domain types, use cases, ports, persistence adapters, and tests close to one another. External adapters call application use cases and do not call Quartz directly.

## 7. Domain model

### 7.1 JobDefinition

A JobDefinition describes the durable work descriptor, not executable Java code.

Required fields:

- `jobId`: UUID generated by the scheduler or supplied for idempotent import.
- `namespace`: stable service or bounded-context identifier.
- `name`: human-readable unique name within a namespace.
- `description`: optional description.
- `destinationId`: reference to an approved destination.
- `destinationVersion`: immutable destination configuration version bound to the job.
- `eventType`: semantic business event name, for example `billing.invoice.due`.
- `payload`: JSON object, maximum 64 KiB after UTF-8 serialization.
- `headers`: string map, maximum 32 entries and 4 KiB total.
- `concurrencyPolicy`: `ALLOW` or `DISALLOW`.
- `recoveryPolicy`: `NONE` or `REQUEST_RECOVERY`.
- `durable`: whether the Quartz job remains after its final trigger is removed; default `true`.
- `state`: `ACTIVE`, `PAUSED`, or `DELETED`.
- `revision`: monotonic optimistic-lock version.
- audit timestamps and creator/updater identifiers.

The Quartz JobDataMap stores only string references such as `jobId`, `namespace`, and `revision`. It must never store Java-serialized payload objects.

### 7.2 TriggerDefinition

A TriggerDefinition belongs to exactly one JobDefinition.

Common fields:

- `triggerId`: UUID.
- `jobId`: owning job.
- `namespace` and `name`.
- `type`: one of the supported trigger types.
- `startAt`: optional instant; defaults to creation time for repeating schedules.
- `endAt`: optional instant.
- `priority`: Quartz priority, default `5`.
- `timezone`: IANA zone for calendar-based types.
- `misfirePolicy`: type-compatible policy.
- `calendarNames`: zero or more registered Quartz calendars to exclude dates.
- `state`: `ACTIVE`, `PAUSED`, `COMPLETE`, or `DELETED`.
- `revision` and audit fields.

Supported trigger specifications:

1. `ONCE`: one execution at an absolute instant.
2. `CRON`: Quartz cron expression plus required IANA timezone.
3. `SIMPLE_INTERVAL`: fixed duration and optional repeat count.
4. `CALENDAR_INTERVAL`: every N days, weeks, months, or years using calendar arithmetic.
5. `DAILY_TIME_INTERVAL`: selected weekdays between daily start and end times at a fixed interval.

The API validates that a misfire policy is legal for the selected trigger type.

### 7.3 DestinationDefinition

Schedules reference a registered destination rather than accepting unrestricted broker coordinates in every request.

V1 destination fields:

- `destinationId` and `version`.
- `namespace` ownership.
- `type = KAFKA`.
- `topic`.
- optional key expression limited to approved variables such as `${jobId}`, `${triggerId}`, `${executionId}`, and `${namespace}`.
- fixed Kafka headers controlled by administrators.
- enabled flag.

Updating a destination creates a new version. Existing jobs remain bound to their original version until explicitly updated, so a destination change cannot silently reroute historical schedules.

### 7.4 ExecutionRecord

An ExecutionRecord represents one logical due occurrence.

Fields include:

- `executionId`: deterministic UUID.
- `jobId`, `triggerId`, namespace, and definition revisions.
- `scheduledFireTime`, `actualFireTime`, and optional previous/next fire times.
- `recovery`: whether Quartz marked the execution as recovering.
- `state`: `PENDING_DELIVERY`, `DELIVERED`, `DELIVERY_FAILED`, or `SUPPRESSED`.
- destination snapshot and event metadata.
- delivery attempts, last error, and timestamps.

The uniqueness boundary is `(trigger_id, scheduled_fire_time, manual_fire_id)`. Normal recurring executions use a null manual ID. A manual fire supplies a UUID so multiple explicit manual fires remain distinct.

## 8. Quartz mapping

### 8.1 Quartz keys

External UUIDs are the stable API identities. Quartz keys are deterministic internal projections:

```text
JobKey.group    = ns:<namespace>:jobs
JobKey.name     = <jobId>
TriggerKey.group= ns:<namespace>:triggers
TriggerKey.name = <triggerId>
```

Human-readable names are metadata, not Quartz identity, so renaming does not require key migration.

### 8.2 Generic job classes

Clients never choose a Quartz class. The scheduler selects one of two internal classes:

- `ConcurrentDispatchQuartzJob` for `concurrencyPolicy=ALLOW`.
- `NonConcurrentDispatchQuartzJob` annotated with `@DisallowConcurrentExecution` for `concurrencyPolicy=DISALLOW`.

Both delegate to the same `ScheduledExecutionService` Spring bean. The selected class is an internal projection of policy, not part of the public contract.

### 8.3 Transaction participation

Spring's `SchedulerFactoryBean` is configured with the application DataSource so it uses `LocalDataSourceJobStore`. Scheduler mutations are executed inside Spring-managed transactions. The same PostgreSQL transaction can therefore update scheduler domain tables and Quartz tables.

Do not configure a second Quartz-owned connection pool and do not manually set a Quartz data-source property when Spring supplies the DataSource. Do not mutate `QRTZ_*` tables directly.

### 8.4 Cluster configuration

All scheduler replicas share:

- one PostgreSQL database and the same Quartz table set;
- the same scheduler name;
- unique `instanceId=AUTO`;
- `isClustered=true`;
- synchronized system clocks;
- identical application and contract versions during rolling deployment compatibility windows.

Initial operational defaults:

- cluster check-in interval: 10 seconds;
- misfire threshold: 60 seconds;
- batch trigger acquisition count: 10;
- acquire triggers within lock: true;
- thread pool size: 10 per node, because each Quartz execution performs only a short local transaction;
- graceful shutdown waits for active jobs for at most the platform termination grace period.

These values are configuration properties and must be load-tested before production scaling.

## 9. Public command model

Every transport maps to the same application commands:

- `CreateJob`
- `UpdateJob`
- `PauseJob`
- `ResumeJob`
- `DeleteJob`
- `CreateTrigger`
- `ReplaceTrigger`
- `PauseTrigger`
- `ResumeTrigger`
- `DeleteTrigger`
- `FireTriggerNow`
- `CreateSchedule` as an atomic convenience command for one job plus one or more triggers

Every command contains:

- `requestId`: UUID used for idempotency across retries and transports.
- `namespace`.
- caller identity.
- optional `expectedRevision` for mutation concurrency control.
- command-specific body.

A command with the same `requestId` and the same canonical payload returns the stored result. Reusing the same request ID with a different payload returns a conflict.

## 10. REST API

Base path: `/api/v1`.

Primary resources:

```text
POST   /jobs
GET    /jobs/{jobId}
PATCH  /jobs/{jobId}
DELETE /jobs/{jobId}
POST   /jobs/{jobId}:pause
POST   /jobs/{jobId}:resume
GET    /jobs/{jobId}/triggers
POST   /jobs/{jobId}/triggers

GET    /triggers/{triggerId}
PUT    /triggers/{triggerId}
DELETE /triggers/{triggerId}
POST   /triggers/{triggerId}:pause
POST   /triggers/{triggerId}:resume
POST   /triggers/{triggerId}:fire

POST   /schedules
GET    /executions/{executionId}
GET    /executions?namespace=&jobId=&triggerId=&from=&to=&state=
```

Mutation requests require an `Idempotency-Key` UUID header. Updates require `If-Match` carrying the current numeric revision. Responses return `ETag` with the new revision.

Example convenience request:

```json
{
  "namespace": "billing",
  "job": {
    "name": "annual-contract-renewal",
    "destinationId": "9c61c37c-4565-4d7e-a69c-ff9dd66d2d31",
    "eventType": "billing.contract.renewal-due",
    "payload": {
      "contractId": "CT-2026-0001"
    },
    "concurrencyPolicy": "DISALLOW",
    "recoveryPolicy": "REQUEST_RECOVERY",
    "durable": true
  },
  "triggers": [
    {
      "name": "renewal-date",
      "type": "CRON",
      "cron": "0 0 8 15 12 ? *",
      "timezone": "Asia/Ho_Chi_Minh",
      "misfirePolicy": "FIRE_ONCE_NOW"
    }
  ]
}
```

The response contains the created job, triggers, revisions, and calculated next fire times.

## 11. gRPC API

The `scheduler-contracts` module defines `SchedulerCommandService` and `SchedulerQueryService` in Protocol Buffers.

Command RPCs mirror REST command semantics. Trigger specifications use `oneof`:

```proto
message TriggerSpec {
  oneof kind {
    OnceTrigger once = 1;
    CronTrigger cron = 2;
    SimpleIntervalTrigger simple_interval = 3;
    CalendarIntervalTrigger calendar_interval = 4;
    DailyTimeIntervalTrigger daily_time_interval = 5;
  }
}
```

`request_id`, namespace, expected revision, and caller metadata have the same meaning as REST. Domain error codes map to stable gRPC statuses and structured error details.

## 12. Kafka command ingestion

Kafka command ingestion supports asynchronous scheduling requests from event-driven services.

Topics:

- `mcs.scheduler.commands.v1`
- `mcs.scheduler.command-results.v1`
- `mcs.scheduler.executions.v1` as the default canonical execution event topic when a destination does not specify a dedicated approved topic

Command envelope:

```json
{
  "schemaVersion": 1,
  "messageId": "uuid",
  "requestId": "uuid",
  "occurredAt": "2026-08-06T02:00:00Z",
  "producer": "billing-service",
  "namespace": "billing",
  "commandType": "CREATE_SCHEDULE",
  "payload": {}
}
```

Kafka records are keyed by namespace plus aggregate identity when available to preserve command ordering for the same job or trigger.

An inbox table deduplicates `messageId`. The listener starts a database transaction, inserts the inbox entry, invokes the same application command handler used by REST/gRPC, and writes a command-result outbox record. The consumer offset is committed only after the transaction succeeds. Poison messages are retried with bounded backoff and then published to a dead-letter topic with the validation or processing error.

## 13. Scheduled execution event

The outbound event contains:

```json
{
  "schemaVersion": 1,
  "executionId": "uuid",
  "namespace": "billing",
  "eventType": "billing.contract.renewal-due",
  "jobId": "uuid",
  "triggerId": "uuid",
  "scheduledFireTime": "2027-12-15T01:00:00Z",
  "actualFireTime": "2027-12-15T01:00:00.137Z",
  "recovery": false,
  "payload": {
    "contractId": "CT-2026-0001"
  },
  "headers": {}
}
```

`executionId` is the consumer idempotency key. Delivery is at least once at the Kafka boundary. The scheduler guarantees that duplicate Quartz firing/recovery attempts produce a single logical execution/outbox record; Kafka consumers must still deduplicate by execution ID before applying non-idempotent business side effects.

## 14. Persistence model

Domain tables use the `scheduler` PostgreSQL schema. Quartz uses its official `QRTZ_*` tables in a separate `quartz` schema or the same schema with the standard prefix; the implementation will use `quartz.QRTZ_*` to separate ownership clearly.

Required domain tables:

- `scheduler.destination`
- `scheduler.job_definition`
- `scheduler.trigger_definition`
- `scheduler.command_request`
- `scheduler.inbox_message`
- `scheduler.execution`
- `scheduler.outbox_event`
- `scheduler.audit_event`

Important constraints:

- unique `(namespace, name)` for live jobs;
- unique `(job_id, name)` for live triggers;
- unique command `request_id` plus canonical request hash;
- unique inbox `message_id`;
- unique normal execution `(trigger_id, scheduled_fire_time)`;
- unique manual execution `manual_fire_id`;
- outbox event ID primary key and status indexes;
- optimistic lock `revision` columns.

Payloads are stored as PostgreSQL `jsonb`. Sensitive secrets are never accepted in schedule payloads. Payload logging is disabled by default and observability records contain identifiers and sizes rather than full payload content.

## 15. State and lifecycle rules

1. Pausing a job pauses all of its Quartz triggers and marks the domain job paused.
2. Resuming a job resumes only triggers that were not individually paused before the job pause. The trigger table therefore tracks `pause_reason` as `JOB`, `TRIGGER`, or null.
3. Pausing one trigger does not pause sibling triggers.
4. Deleting a trigger unschedules it. If it was the final trigger, a durable job remains; a non-durable job is removed.
5. Deleting a job requires `cascade=true` when triggers still exist.
6. Replacing a trigger preserves its trigger UUID and Quartz key but increments the revision and uses Quartz rescheduling atomically.
7. Updating a job payload affects future executions only. Existing execution rows and outbox messages retain their snapshots.
8. A completed one-shot trigger remains queryable in the domain history even if Quartz removes the completed trigger row.

## 16. Validation rules

- All instants are ISO-8601 UTC values at boundaries; calendar trigger timezone uses an IANA zone.
- `endAt` must be after `startAt`.
- Cron expressions are parsed by Quartz before persistence.
- A calculated next fire time must exist unless the caller explicitly creates a historical/complete definition for migration purposes; migration mode is not part of V1.
- Names contain lowercase letters, digits, dots, hyphens, or underscores and are at most 128 characters.
- Namespace is at most 64 characters.
- Repeat intervals have configured minimums to prevent Quartz from becoming a sub-second task queue. Initial minimum is one second.
- Limits on active jobs and triggers per namespace are configuration-backed quotas.

## 17. Error model

Stable domain codes include:

- `JOB_NOT_FOUND`
- `TRIGGER_NOT_FOUND`
- `DESTINATION_NOT_FOUND`
- `REVISION_CONFLICT`
- `IDEMPOTENCY_CONFLICT`
- `INVALID_TRIGGER_SPEC`
- `INVALID_MISFIRE_POLICY`
- `SCHEDULE_HAS_NO_FUTURE_FIRE_TIME`
- `JOB_HAS_TRIGGERS`
- `NAMESPACE_QUOTA_EXCEEDED`
- `SCHEDULER_UNAVAILABLE`

REST returns RFC 9457 problem details. gRPC returns matching status codes plus typed error details. Kafka returns a command-result event with the same domain code and routes malformed envelopes to the dead-letter topic.

## 18. Reconciliation and drift handling

Although scheduler domain and Quartz writes share a local transaction, a reconciliation component detects operational drift caused by manual database changes, incompatible deployments, or partial migrations.

It periodically checks bounded pages of active definitions:

- domain job exists but Quartz JobKey is missing;
- Quartz job exists but domain definition is absent;
- active domain trigger is missing or has a different next fire time/spec hash;
- completed one-shot Quartz trigger should update domain state;
- trigger references the wrong job key.

The default mode reports metrics and audit events only. An explicit admin repair command performs idempotent reconstruction from the domain definition. No automatic destructive deletion is performed.

## 19. Observability

Expose:

- Actuator health for PostgreSQL, Kafka, and Quartz scheduler state.
- gauges for scheduler nodes, active jobs, active triggers, paused triggers, and outbox backlog;
- counters for commands, command failures, Quartz firings, suppressed duplicate firings, misfires, recoveries, outbox publishes, retries, and dead-letter events;
- timers for command latency, database/Quartz mutation latency, trigger acquisition-to-execution delay, and outbox publish delay;
- structured logs with request ID, job ID, trigger ID, execution ID, namespace, and Quartz instance ID;
- OpenTelemetry trace propagation from REST/gRPC/Kafka commands into command result events. Scheduled execution creates a new trace linked to the schedule creation/update trace when link metadata is available.

## 20. Security boundary

The core design provides a `CallerIdentity` and `NamespaceAuthorizer` port. Development mode accepts a configured local identity. Production deployments plug in platform authentication without changing command handlers.

Authorization rules:

- a caller may mutate only namespaces assigned to it;
- destination registration is an administrator operation;
- query access is namespace scoped;
- Kafka broker ACLs restrict command producers and execution consumers;
- arbitrary Kafka topics cannot be selected unless registered as an enabled destination.

## 21. Testing strategy

### 21.1 Unit tests

Use plain JUnit 5, AssertJ, and Mockito for:

- trigger validation and Quartz mapping;
- idempotency hash handling;
- state transitions and pause reasons;
- deterministic execution ID generation;
- domain-to-REST/gRPC/Kafka mappings;
- outbox publisher retry state transitions.

### 21.2 Adapter contract tests

- MockMvc tests for REST request validation, status codes, problem details, ETag, and idempotency headers.
- In-process gRPC server tests for protobuf mapping, status details, and metadata.
- Embedded serializer/deserializer tests for Kafka envelopes and version compatibility.

### 21.3 Testcontainers integration tests

Use reusable test fixtures for:

- PostgreSQL with Flyway migrations and real Quartz tables;
- Apache Kafka native container;
- application context wired to both containers.

Integration cases include:

- atomic job plus trigger creation;
- restart persistence;
- cron and calendar interval next-fire calculation;
- pause/resume/reschedule/delete;
- duplicate REST/gRPC/Kafka request IDs;
- Kafka inbox deduplication;
- execution/outbox atomicity;
- outbox retry and eventual publish;
- consumer-visible execution event schema.

### 21.4 Cluster tests

Run two scheduler application instances against one PostgreSQL container and one Kafka container. Schedule many one-shot jobs and assert that every `(triggerId, scheduledFireTime)` creates exactly one execution row.

A separate containerized failover test starts two real service containers, blocks a recovery-enabled test job after Quartz acquisition, kills the owning container, waits beyond the configured cluster check-in window, and asserts that the surviving node recovers the firing without creating a second logical execution.

### 21.5 Migration tests

Every Flyway migration is applied from an empty database. Upgrade tests restore the previous release schema, apply new migrations, start two scheduler nodes, and verify existing schedules remain readable and fireable.

## 22. Deployment model

Production minimum:

- two scheduler replicas;
- one highly available PostgreSQL cluster;
- Kafka with replication appropriate to platform policy;
- synchronized clocks;
- rolling deployments with backward-compatible database and message changes;
- PodDisruptionBudget or equivalent to avoid voluntarily stopping all nodes at once.

Scheduler replicas are stateless outside PostgreSQL. Horizontal scaling increases trigger acquisition and short execution capacity, but PostgreSQL locking becomes the limiting resource before CPU in many workloads. Scale must be based on measured trigger count, firing rate, misfire recovery, and lock wait metrics.

## 23. Delivery phases

1. Foundation, schema, and Quartz cluster configuration.
2. Job/trigger domain and transactional Quartz adapter.
3. REST control plane.
4. gRPC control plane.
5. Kafka command ingestion with inbox/result outbox.
6. Scheduled execution recording and Kafka delivery outbox.
7. lifecycle queries, reconciliation, observability, and client SDK.
8. two-node cluster and process-kill failover verification.
9. Docker/CI/runbooks and production hardening.

## 24. Acceptance criteria

The initial release is complete when all of the following are demonstrated automatically:

1. A job and cron trigger created through REST survive restart and fire on one of two scheduler nodes.
2. The same model can be created through gRPC and Kafka commands with equivalent results.
3. One job can own multiple independently managed triggers.
4. A due trigger creates one logical execution and one durable outbox event even when Quartz attempts recovery.
5. The execution event is delivered to Kafka and contains a stable execution idempotency key.
6. Duplicate request IDs return the original result and conflicting request reuse is rejected.
7. Pause, resume, replace trigger, delete, and manual fire semantics are verified.
8. PostgreSQL and Kafka Testcontainers cover persistence, messaging, and restart behavior.
9. A two-node cluster test proves no duplicate logical execution under normal load.
10. A process-kill test proves recovery after a scheduler node failure.
11. Metrics and health endpoints expose scheduler, cluster, misfire, execution, and outbox state.
12. No public request can submit a Java class name, SQL fragment, secret, or unregistered Kafka destination.

## 25. Key decisions summary

- PostgreSQL domain tables are the control-plane source of truth; Quartz tables are the execution projection.
- Spring-managed `LocalDataSourceJobStore` provides local transaction participation for domain and Quartz mutations.
- Job and Trigger remain separate first-class resources.
- Generic internal Quartz jobs emit durable execution events; consumer business code stays outside the scheduler.
- Transactional inbox/outbox patterns provide idempotent asynchronous command and delivery behavior.
- V1 supports Kafka outbound delivery only, behind a destination port that can add HTTP or gRPC later.
- Cluster safety is verified with real two-node and process-kill tests, not only mocked Scheduler calls.
