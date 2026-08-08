# MCS Scheduler

MCS Scheduler is a clustered Spring Boot scheduling service backed by PostgreSQL Quartz JDBC JobStore and Kafka. Domain definitions are stored in the `scheduler` schema, Quartz owns the `quartz.QRTZ_*` projection, and every scheduled occurrence is reduced to one durable logical execution before an outbox publisher delivers the registered Kafka event.

## Architecture

```text
REST / gRPC / Kafka commands
          |
          v
SchedulerCommandFacade ----> scheduler.* domain tables
          |
          v
Quartz projection ----------> quartz.QRTZ_* tables
          |
          v
clustered Quartz nodes
          |
          v
ScheduledExecutionService --> scheduler.execution + scheduler.outbox_event
                                      |
                                      v
                                    Kafka
```

REST, gRPC, and Kafka all delegate to the same command facade. Quartz uses `instanceId=AUTO`, a shared scheduler name, JDBC clustering, and the same PostgreSQL database on every node. Never mutate `quartz.QRTZ_*` directly; domain mutations go through the scheduler APIs and reconciliation is report/repair driven.

## Job versus Trigger

A **Job** describes what durable event should be emitted: namespace, destination/version, event type, payload, concurrency policy, and recovery policy. A **Trigger** describes when that job should fire. One job can own multiple independent triggers.

Example: one `billing.invoice-reminder` job can have a daily 08:00 cron trigger and a monthly calendar-interval trigger. Updating the event payload belongs to the Job; changing the cadence belongs to the Trigger.

## Local two-node deployment

```bash
docker compose -f docker/compose.yml up --build -d
docker compose -f docker/compose.yml ps
```

Endpoints:

- node 1 REST `http://localhost:8081`, gRPC `localhost:9091`
- node 2 REST `http://localhost:8082`, gRPC `localhost:9092`
- Kafka external bootstrap `localhost:29092`
- optional Kafka console: `docker compose -f docker/compose.yml --profile tools up -d kafka-console`, then `http://localhost:8085`

Stop the topology with:

```bash
docker compose -f docker/compose.yml down
# add -v only when intentionally discarding scheduler/Quartz state
docker compose -f docker/compose.yml down -v
```

## Create a schedule through REST

A destination must exist before a Job references it. With a valid destination UUID, create a recovery-enabled one-shot schedule:

```bash
curl -i http://localhost:8081/api/v1/schedules \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "caller":"readme",
    "job":{
      "jobId":"11111111-1111-1111-1111-111111111111",
      "namespace":"billing",
      "name":"invoice-dispatch",
      "description":"Dispatch invoice event",
      "destinationId":"44444444-4444-4444-4444-444444444444",
      "destinationVersion":1,
      "eventType":"invoice.due",
      "payload":{"invoiceId":"INV-1"},
      "headers":{},
      "concurrencyPolicy":"ALLOW",
      "recoveryPolicy":"REQUEST_RECOVERY",
      "durable":true
    },
    "triggers":[{
      "triggerId":"33333333-3333-3333-3333-333333333333",
      "jobId":"11111111-1111-1111-1111-111111111111",
      "namespace":"billing",
      "name":"invoice-once",
      "description":"One-shot invoice dispatch",
      "spec":{"type":"ONCE","fireAt":"2026-08-09T01:00:00Z"},
      "startAt":"2026-08-09T01:00:00Z",
      "endAt":null,
      "priority":5,
      "timezone":"UTC",
      "misfirePolicy":"FIRE_NOW",
      "calendarNames":[]
    }]
  }'
```

Replay the exact same idempotency key and payload to receive the stored command result instead of creating duplicate definitions.

## Create a schedule through gRPC

The protobuf contract is under `scheduler-contracts/src/main/proto`. With `grpcurl` and a request JSON file:

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_command.proto \
  -d @ localhost:9091 \
  mcs.scheduler.v1.SchedulerCommandService/CreateSchedule <<'JSON'
{
  "requestId":"55555555-5555-5555-5555-555555555555",
  "namespace":"billing",
  "caller":"readme",
  "job":{
    "jobId":"11111111-1111-1111-1111-111111111111",
    "name":"invoice-dispatch",
    "description":"Dispatch invoice event",
    "destinationId":"44444444-4444-4444-4444-444444444444",
    "destinationVersion":"1",
    "eventType":"invoice.due",
    "payload":{"invoiceId":"INV-1"},
    "concurrencyPolicy":"CONCURRENCY_POLICY_ALLOW",
    "recoveryPolicy":"RECOVERY_POLICY_REQUEST_RECOVERY",
    "durable":true
  },
  "triggers":[{
    "triggerId":"33333333-3333-3333-3333-333333333333",
    "jobId":"11111111-1111-1111-1111-111111111111",
    "name":"invoice-once",
    "description":"One-shot invoice dispatch",
    "spec":{"once":{"fireAt":"2026-08-09T01:00:00Z"}},
    "startAt":"2026-08-09T01:00:00Z",
    "priority":5,
    "misfirePolicy":"MISFIRE_POLICY_FIRE_NOW"
  }]
}
JSON
```

## Create a schedule through Kafka

Publish to `mcs.scheduler.commands.v1` using key `<namespace>:<aggregateId>` and the versioned envelope below. The `payload.job` and `payload.triggers` use the same JSON model as REST.

```json
{
  "schemaVersion": 1,
  "messageId": "66666666-6666-6666-6666-666666666666",
  "requestId": "77777777-7777-7777-7777-777777777777",
  "occurredAt": "2026-08-08T09:00:00Z",
  "producer": "readme",
  "namespace": "billing",
  "commandType": "CREATE_SCHEDULE",
  "payload": {
    "job": {
      "jobId": "11111111-1111-1111-1111-111111111111",
      "namespace": "billing",
      "name": "invoice-dispatch",
      "description": "Dispatch invoice event",
      "destinationId": "44444444-4444-4444-4444-444444444444",
      "destinationVersion": 1,
      "eventType": "invoice.due",
      "payload": {"invoiceId": "INV-1"},
      "headers": {},
      "concurrencyPolicy": "ALLOW",
      "recoveryPolicy": "REQUEST_RECOVERY",
      "durable": true
    },
    "triggers": [{
      "triggerId": "33333333-3333-3333-3333-333333333333",
      "jobId": "11111111-1111-1111-1111-111111111111",
      "namespace": "billing",
      "name": "invoice-once",
      "description": "One-shot invoice dispatch",
      "spec": {"type": "ONCE", "fireAt": "2026-08-09T01:00:00Z"},
      "startAt": "2026-08-09T01:00:00Z",
      "endAt": null,
      "priority": 5,
      "timezone": "UTC",
      "misfirePolicy": "FIRE_NOW",
      "calendarNames": []
    }]
  }
}
```

Command results are delivered through `mcs.scheduler.command-results.v1`; permanent invalid commands go to `mcs.scheduler.commands.v1.DLT` with source and stable error metadata.

## Java client module

Add `scheduler-client` with the same project version. Enable exactly one transport.

Synchronous gRPC:

```yaml
mcs:
  scheduler:
    client:
      enabled: true
      transport: grpc
      grpc-target: scheduler
      producer: billing-service
```

Then inject `io.k2iot.mcs.scheduler.client.SchedulerClient` and call `createSchedule(request, requestId)`, pause/resume/delete, or manual fire methods.

Asynchronous Kafka:

```yaml
mcs:
  scheduler:
    client:
      enabled: true
      transport: kafka
      kafka-command-topic: mcs.scheduler.commands.v1
      producer: billing-service
```

Inject `AsyncSchedulerClient` for Kafka command publication.

## Verification

```bash
./mvnw spotless:check verify
for i in 1 2 3; do \
  ./mvnw -pl scheduler-service -am -Dit.test=RestartPersistenceIT,TwoNodeQuartzClusterIT verify || exit 1; \
done
./mvnw -pl scheduler-service -am -Dit.test=ProcessKillRecoveryIT verify
docker compose -f docker/compose.yml config
```

`ProcessKillRecoveryIT` builds the packaged service image, starts PostgreSQL/Kafka/two scheduler containers, blocks the node that acquired a recovery-enabled firing before the execution transaction, kills that container with Docker, and asserts the surviving Quartz node records and publishes exactly one recovered logical execution.

## Operations runbook

See [docs/operations-runbook.md](docs/operations-runbook.md) for PostgreSQL/Kafka outage behavior, Quartz failover interpretation, misfire diagnosis, outbox repair, rolling deployments, clock requirements, and reconciliation procedures.
