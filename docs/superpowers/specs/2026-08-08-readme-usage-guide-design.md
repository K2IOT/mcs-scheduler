# README Usage Guide Design

## Goal

Turn `README.md` into a self-contained usage guide that lets a developer clone the repository, start the local cluster, provision prerequisites, create and manage schedules through REST/gRPC/Kafka, query state, consume execution events, use the Java client, and verify the deployment without reading implementation code.

## Scope

The README will keep the existing architecture and operations overview, but reorganize usage around an executable developer journey.

### 1. Five-minute quick start

Document one end-to-end path:

1. start PostgreSQL, Kafka, and two scheduler nodes with Docker Compose;
2. verify both REST nodes and the gRPC endpoint;
3. provision a Kafka destination;
4. create one Job with two Triggers;
5. query the Job/Triggers through gRPC;
6. manually fire or wait for an execution;
7. consume the emitted Kafka event;
8. tear the topology down safely.

All IDs, topics, ports, and command examples must be mutually consistent so the section is copy/paste friendly.

### 2. Destination provisioning

The existing README requires a destination but does not explain how to create one. Add a concrete supported provisioning example and explain the destination fields used by Jobs: namespace, destination ID/version, topic, key expression, headers, and enabled state.

If Destination does not have a public command API, document the supported bootstrap path explicitly instead of inventing an endpoint.

### 3. REST cookbook

Add runnable curl examples for the public command surface:

- CreateSchedule
- CreateJob
- UpdateJob with `ETag` / `If-Match`
- PauseJob / ResumeJob / DeleteJob
- CreateTrigger
- ReplaceTrigger
- PauseTrigger / ResumeTrigger / DeleteTrigger
- FireTriggerNow

Explain that every command requires an idempotency request identifier, and mutation commands that change an existing revision require optimistic concurrency through `If-Match`.

### 4. gRPC cookbook

Keep CreateSchedule and add representative command/query examples using `grpcurl`:

Commands:
- CreateSchedule
- Pause/Resume or FireTriggerNow as a mutation example

Queries:
- GetJob
- ListJobTriggers
- GetTrigger
- GetExecution
- ListExecutions including pagination fields

The README should point to the proto directory as the authoritative contract rather than duplicating every RPC body.

### 5. Kafka cookbook

Turn the current JSON-only section into runnable CLI examples:

- publish a command to `mcs.scheduler.commands.v1` with the required message key;
- consume `mcs.scheduler.command-results.v1`;
- inspect `mcs.scheduler.commands.v1.DLT`;
- consume a registered destination event topic.

Document the versioned envelope fields, request/message IDs, stable error metadata, and command-result correlation.

### 6. Idempotency and optimistic concurrency

Add explicit examples for:

- same request ID + same payload => stored/cached result, no duplicate definition;
- same request ID + changed payload => `IDEMPOTENCY_CONFLICT`;
- stale `If-Match` revision => concurrency failure;
- successful mutation with the latest ETag.

### 7. Trigger recipes

Document at least one example per supported trigger specification present in the contracts/domain, with ONCE and CRON required. Additional calendar/interval recipes are included only if they are actually supported by the current codebase.

### 8. Java client

Expand the current YAML-only section with:

- Maven dependency coordinates;
- synchronous gRPC client configuration;
- minimal Java `SchedulerClient` call;
- asynchronous Kafka client configuration and publication example;
- request ID handling expectations.

### 9. Configuration reference

Add a compact table for the most important environment/configuration values:

- datasource URL/user/password;
- Kafka bootstrap servers;
- scheduler instance ID;
- command/result/DLT topics;
- outbox tuning;
- REST/gRPC ports;
- Quartz cluster timing relevant to operators.

Do not duplicate every Spring property; link to `application.yml` for exhaustive defaults.

### 10. Observability and verification

Document:

- `/actuator/health`;
- Prometheus endpoint;
- how to confirm both Quartz nodes are present in shared PostgreSQL;
- `grpcurl` transport probe;
- current CI verification commands/gates introduced by Final Acceptance.

The README must reflect `final-acceptance`, `compose-smoke`, `migration-compatibility`, cluster, process-kill recovery, image build, integration, and aggregate validation coverage.

### 11. Operations boundary

Keep detailed failure recovery, reconciliation, outage, and rolling-deployment procedures in `docs/operations-runbook.md`. README should summarize and link rather than duplicate the full runbook.

## Documentation structure

Recommended order:

1. Project summary
2. Architecture
3. Concepts: Job vs Trigger vs Destination
4. Five-minute Quick Start
5. REST Cookbook
6. gRPC Cookbook
7. Kafka Cookbook
8. Idempotency & ETag behavior
9. Trigger recipes
10. Java client
11. Configuration reference
12. Observability & verification
13. Operations runbook

## Correctness rules

- Every command example must correspond to an API/RPC/topic that exists on current `main`.
- Do not invent a public Destination API if none exists.
- Reuse one coherent set of UUIDs/topics throughout the quick start.
- Use future-safe timestamps or shell-generated timestamps where practical so examples do not expire.
- Examples that mutate a resource must show how the required ETag/revision is obtained.
- Kafka examples must include the record key where ordering/partition affinity depends on it.
- Java examples must use classes/methods that exist in `scheduler-client`.
- Keep secrets out of examples and logs.

## Verification

After implementation:

- review every README command against controller/proto/client code;
- run `./mvnw -B -ntp -DskipITs spotless:check verify` to ensure documentation changes do not introduce repository regressions;
- run `docker compose -f docker/compose.yml config`;
- rely on existing CI final-acceptance/compose gates for full packaged topology behavior.

## Out of scope

- adding new business APIs solely to make documentation easier;
- redesigning scheduler domain behavior;
- duplicating the operations runbook in README;
- documenting unsupported trigger types or speculative configuration.