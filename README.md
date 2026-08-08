# MCS Scheduler

MCS Scheduler is a clustered Spring Boot scheduling service backed by PostgreSQL Quartz JDBC JobStore and Kafka. Domain definitions live in the `scheduler` schema, Quartz owns the `quartz.QRTZ_*` projection, and each scheduled occurrence becomes one durable logical execution before the outbox publisher delivers the registered Kafka event.

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

REST, gRPC, and Kafka delegate to the same command facade. Quartz uses `instanceId=AUTO`, a shared scheduler name, JDBC clustering, and the same PostgreSQL database on every node.

Never mutate `quartz.QRTZ_*` directly. Domain mutations go through scheduler command surfaces; reconciliation is report/repair driven.

## Core concepts

A **Destination** tells the scheduler where an execution event must be delivered. In V1 the implemented destination type is Kafka and a Job references a specific `(destinationId, destinationVersion)`.

A **Job** describes what durable event should be emitted: namespace, destination/version, event type, payload, headers, concurrency policy, and recovery policy.

A **Trigger** describes when a Job fires. One Job can own multiple independent Triggers.

Example: one `billing.invoice-dispatch` Job can have a one-shot Trigger for the next invoice run and a cron Trigger for recurring reminders. Updating the event payload belongs to the Job; changing cadence belongs to the Trigger.

## Local topology

Start the production-like two-node topology:

```bash
docker compose -f docker/compose.yml up --build -d
docker compose -f docker/compose.yml ps
```

Endpoints:

- node 1 REST: `http://localhost:8081`
- node 1 gRPC: `localhost:9091`
- node 2 REST: `http://localhost:8082`
- node 2 gRPC: `localhost:9092`
- Kafka external bootstrap: `localhost:29092`
- optional Kafka console: `docker compose -f docker/compose.yml --profile tools up -d kafka-console`, then open `http://localhost:8085`

Stop while keeping PostgreSQL state:

```bash
docker compose -f docker/compose.yml down
```

Delete local scheduler/Quartz state only when that is intentional:

```bash
docker compose -f docker/compose.yml down -v
```

# Five-minute Quick Start

The following flow uses one coherent set of IDs and can be copied from the repository root.

```text
namespace:   billing
destination: 44444444-4444-4444-4444-444444444444
job:         11111111-1111-1111-1111-111111111111
trigger 1:   33333333-3333-3333-3333-333333333333
trigger 2:   33333333-3333-3333-3333-333333333334
topic:       billing.invoice-events.v1
```

## 1. Start and verify both scheduler nodes

```bash
docker compose -f docker/compose.yml up --build -d

curl -fsS http://localhost:8081/actuator/health
curl -fsS http://localhost:8082/actuator/health
```

Both calls should return an `UP` health status.

Verify that the shared Quartz database currently sees two scheduler instances:

```bash
docker compose -f docker/compose.yml exec -T postgres \
  psql -U scheduler -d scheduler -c \
  'select scheduler_name, instance_name, last_checkin_time from quartz.qrtz_scheduler_state order by instance_name;'
```

## 2. Bootstrap a Kafka Destination

A Destination must exist before a Job can reference it. V1 does not expose a public Destination command API; local/development provisioning is performed administratively in PostgreSQL.

Create the destination topic first:

```bash
docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic billing.invoice-events.v1 \
  --partitions 1 \
  --replication-factor 1
```

Register the Destination:

```bash
docker compose -f docker/compose.yml exec -T postgres \
  psql -U scheduler -d scheduler <<'SQL'
INSERT INTO scheduler.destination (
    destination_id,
    version,
    namespace,
    type,
    topic,
    key_expression,
    headers,
    enabled,
    created_by,
    updated_by
) VALUES (
    '44444444-4444-4444-4444-444444444444',
    1,
    'billing',
    'KAFKA',
    'billing.invoice-events.v1',
    'execution:${executionId}',
    '{}'::jsonb,
    true,
    'readme',
    'readme'
)
ON CONFLICT (destination_id, version) DO UPDATE SET
    namespace = EXCLUDED.namespace,
    type = EXCLUDED.type,
    topic = EXCLUDED.topic,
    key_expression = EXCLUDED.key_expression,
    headers = EXCLUDED.headers,
    enabled = EXCLUDED.enabled,
    updated_at = now(),
    updated_by = 'readme';
SQL
```

Production Destination lifecycle should be treated as controlled configuration/administration. Do not invent application-owned rows or update a version already referenced by Jobs without an explicit rollout plan.

## 3. Create one Job with two Triggers through REST

Generate future UTC timestamps so the example does not expire:

```bash
FIRE_AT_1=$(python3 -c 'from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)+timedelta(minutes=2)).isoformat(timespec="seconds").replace("+00:00","Z"))')
FIRE_AT_2=$(python3 -c 'from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)+timedelta(minutes=4)).isoformat(timespec="seconds").replace("+00:00","Z"))')
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
```

Create the schedule:

```bash
curl -i http://localhost:8081/api/v1/schedules \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -d @- <<JSON
{
  "caller": "readme",
  "job": {
    "jobId": "11111111-1111-1111-1111-111111111111",
    "namespace": "billing",
    "name": "invoice-dispatch",
    "description": "Dispatch invoice event",
    "destinationId": "44444444-4444-4444-4444-444444444444",
    "destinationVersion": 1,
    "eventType": "invoice.due",
    "payload": {"invoiceId": "INV-1"},
    "headers": {"source": "readme"},
    "concurrencyPolicy": "ALLOW",
    "recoveryPolicy": "REQUEST_RECOVERY",
    "durable": true
  },
  "triggers": [
    {
      "triggerId": "33333333-3333-3333-3333-333333333333",
      "jobId": "11111111-1111-1111-1111-111111111111",
      "namespace": "billing",
      "name": "invoice-first-run",
      "description": "First one-shot run",
      "spec": {"type": "ONCE", "fireAt": "${FIRE_AT_1}"},
      "startAt": "${FIRE_AT_1}",
      "endAt": null,
      "priority": 5,
      "timezone": "UTC",
      "misfirePolicy": "FIRE_NOW",
      "calendarNames": []
    },
    {
      "triggerId": "33333333-3333-3333-3333-333333333334",
      "jobId": "11111111-1111-1111-1111-111111111111",
      "namespace": "billing",
      "name": "invoice-second-run",
      "description": "Second one-shot run",
      "spec": {"type": "ONCE", "fireAt": "${FIRE_AT_2}"},
      "startAt": "${FIRE_AT_2}",
      "endAt": null,
      "priority": 5,
      "timezone": "UTC",
      "misfirePolicy": "FIRE_NOW",
      "calendarNames": []
    }
  ]
}
JSON
```

The response is `201 Created`, contains the Job and both Triggers, and includes an `ETag` for the Job revision.

Replay the exact same `REQUEST_ID` with the exact same body and the scheduler returns the stored command result instead of creating duplicate definitions.

## 4. Read the Job and Triggers through gRPC

The protobuf contracts are under `scheduler-contracts/src/main/proto`.

Get the Job:

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{"namespace":"billing","jobId":"11111111-1111-1111-1111-111111111111"}' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/GetJob
```

List its Triggers:

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{"namespace":"billing","jobId":"11111111-1111-1111-1111-111111111111","pageSize":100}' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/ListJobTriggers
```

## 5. Fire a Trigger immediately

Manual fire does not modify the Trigger revision.

```bash
MANUAL_FIRE_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -i http://localhost:8081/api/v1/executions \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -d @- <<JSON
{
  "caller": "readme",
  "namespace": "billing",
  "triggerId": "33333333-3333-3333-3333-333333333333",
  "manualFireId": "${MANUAL_FIRE_ID}"
}
JSON
```

## 6. Consume the delivered execution event

```bash
docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic billing.invoice-events.v1 \
  --from-beginning \
  --max-messages 1
```

The event is published from the durable outbox after the logical execution is recorded.

# REST Cookbook

Every REST command requires an `Idempotency-Key` containing a UUID. Mutations of existing Job/Trigger definitions also require `If-Match` with the current positive revision, normally obtained from the previous REST response `ETag`.

## Create a Job

The example below uses a second Job so it can be run after the Quick Start.

```bash
JOB_ID=22222222-2222-2222-2222-222222222222
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-job.headers -o /tmp/mcs-job.json \
  http://localhost:8081/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -d @- <<JSON
{
  "caller": "readme",
  "job": {
    "jobId": "${JOB_ID}",
    "namespace": "billing",
    "name": "invoice-reminder",
    "description": "Reminder event",
    "destinationId": "44444444-4444-4444-4444-444444444444",
    "destinationVersion": 1,
    "eventType": "invoice.reminder",
    "payload": {"kind": "reminder"},
    "headers": {},
    "concurrencyPolicy": "ALLOW",
    "recoveryPolicy": "NONE",
    "durable": true
  }
}
JSON

cat /tmp/mcs-job.json
JOB_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-job.headers)
echo "JOB_ETAG=${JOB_ETAG}"
```

## Update a Job

The body contains the complete Job draft and its `jobId` must match the path.

```bash
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-job-update.headers -o /tmp/mcs-job-update.json \
  -X PUT "http://localhost:8081/api/v1/jobs/${JOB_ID}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${JOB_ETAG}" \
  -d @- <<JSON
{
  "caller": "readme",
  "job": {
    "jobId": "${JOB_ID}",
    "namespace": "billing",
    "name": "invoice-reminder",
    "description": "Updated reminder event",
    "destinationId": "44444444-4444-4444-4444-444444444444",
    "destinationVersion": 1,
    "eventType": "invoice.reminder",
    "payload": {"kind": "reminder", "version": 2},
    "headers": {},
    "concurrencyPolicy": "ALLOW",
    "recoveryPolicy": "NONE",
    "durable": true
  }
}
JSON

JOB_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-job-update.headers)
echo "new JOB_ETAG=${JOB_ETAG}"
```

## Pause and resume a Job

Each successful mutation returns a new `ETag`; use it for the next mutation.

```bash
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-job-pause.headers -o /tmp/mcs-job-pause.json \
  -X POST "http://localhost:8081/api/v1/jobs/${JOB_ID}/pause" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${JOB_ETAG}" \
  -d '{"namespace":"billing","caller":"readme"}'

JOB_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-job-pause.headers)

REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-job-resume.headers -o /tmp/mcs-job-resume.json \
  -X POST "http://localhost:8081/api/v1/jobs/${JOB_ID}/resume" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${JOB_ETAG}" \
  -d '{"namespace":"billing","caller":"readme"}'

JOB_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-job-resume.headers)
```

## Create a Trigger

```bash
TRIGGER_ID=55555555-5555-5555-5555-555555555555
TRIGGER_AT=$(python3 -c 'from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)+timedelta(minutes=10)).isoformat(timespec="seconds").replace("+00:00","Z"))')
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-trigger.headers -o /tmp/mcs-trigger.json \
  http://localhost:8081/api/v1/triggers \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -d @- <<JSON
{
  "caller": "readme",
  "trigger": {
    "triggerId": "${TRIGGER_ID}",
    "jobId": "${JOB_ID}",
    "namespace": "billing",
    "name": "invoice-reminder-once",
    "description": "One-shot reminder",
    "spec": {"type": "ONCE", "fireAt": "${TRIGGER_AT}"},
    "startAt": "${TRIGGER_AT}",
    "endAt": null,
    "priority": 5,
    "timezone": "UTC",
    "misfirePolicy": "FIRE_NOW",
    "calendarNames": []
  }
}
JSON

TRIGGER_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-trigger.headers)
echo "TRIGGER_ETAG=${TRIGGER_ETAG}"
```

## Replace a Trigger

```bash
REPLACEMENT_AT=$(python3 -c 'from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat(timespec="seconds").replace("+00:00","Z"))')
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-trigger-replace.headers -o /tmp/mcs-trigger-replace.json \
  -X PUT "http://localhost:8081/api/v1/triggers/${TRIGGER_ID}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${TRIGGER_ETAG}" \
  -d @- <<JSON
{
  "caller": "readme",
  "trigger": {
    "triggerId": "${TRIGGER_ID}",
    "jobId": "${JOB_ID}",
    "namespace": "billing",
    "name": "invoice-reminder-once",
    "description": "Rescheduled one-shot reminder",
    "spec": {"type": "ONCE", "fireAt": "${REPLACEMENT_AT}"},
    "startAt": "${REPLACEMENT_AT}",
    "endAt": null,
    "priority": 5,
    "timezone": "UTC",
    "misfirePolicy": "FIRE_NOW",
    "calendarNames": []
  }
}
JSON

TRIGGER_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-trigger-replace.headers)
```

## Pause and resume a Trigger

```bash
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-trigger-pause.headers -o /tmp/mcs-trigger-pause.json \
  -X POST "http://localhost:8081/api/v1/triggers/${TRIGGER_ID}/pause" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${TRIGGER_ETAG}" \
  -d '{"namespace":"billing","caller":"readme"}'

TRIGGER_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-trigger-pause.headers)

REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -sS -D /tmp/mcs-trigger-resume.headers -o /tmp/mcs-trigger-resume.json \
  -X POST "http://localhost:8081/api/v1/triggers/${TRIGGER_ID}/resume" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${TRIGGER_ETAG}" \
  -d '{"namespace":"billing","caller":"readme"}'

TRIGGER_ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/mcs-trigger-resume.headers)
```

## Fire a Trigger now

```bash
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
MANUAL_FIRE_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -i http://localhost:8081/api/v1/executions \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -d "{\"caller\":\"readme\",\"namespace\":\"billing\",\"triggerId\":\"${TRIGGER_ID}\",\"manualFireId\":\"${MANUAL_FIRE_ID}\"}"
```

## Delete a Trigger and Job

Delete the Trigger first when the Job still owns it.

```bash
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -i -X DELETE "http://localhost:8081/api/v1/triggers/${TRIGGER_ID}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${TRIGGER_ETAG}" \
  -d '{"namespace":"billing","caller":"readme"}'
```

Use the Job's latest ETag/revision for deletion:

```bash
REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl -i -X DELETE "http://localhost:8081/api/v1/jobs/${JOB_ID}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${REQUEST_ID}" \
  -H "If-Match: ${JOB_ETAG}" \
  -d '{"namespace":"billing","caller":"readme"}'
```

# Idempotency and optimistic concurrency

The scheduler treats a request ID as the identity of one logical command across REST, gRPC, and Kafka.

| Case | Expected behavior |
|---|---|
| same request ID + same command payload | stored command result is returned; no duplicate definitions |
| same request ID + changed command payload | `IDEMPOTENCY_CONFLICT`; REST returns HTTP `409 Conflict` |
| latest revision in `If-Match` | mutation succeeds and returns the next revision/ETag |
| stale revision in `If-Match` | `REVISION_CONFLICT`; REST returns HTTP `412 Precondition Failed` |

REST accepts numeric ETags such as `"1"` and weak ETags such as `W/"1"`.

If a caller loses the REST ETag, read the Job or Trigger through gRPC and use its `revision` as the next `If-Match` value.

# Trigger recipes

REST accepts five Trigger `spec.type` values.

## ONCE

```json
{
  "type": "ONCE",
  "fireAt": "2030-01-01T00:00:00Z"
}
```

## CRON

Quartz cron expression every five minutes:

```json
{
  "type": "CRON",
  "expression": "0 0/5 * * * ?"
}
```

Set the Trigger draft's top-level `timezone`, for example `"Asia/Ho_Chi_Minh"`.

## SIMPLE_INTERVAL

```json
{
  "type": "SIMPLE_INTERVAL",
  "interval": "PT30S",
  "repeatCount": 10
}
```

`interval` is an ISO-8601 duration. Omit `repeatCount` when an unbounded repeating interval is desired by the domain policy.

## CALENDAR_INTERVAL

```json
{
  "type": "CALENDAR_INTERVAL",
  "interval": 1,
  "unit": "DAYS"
}
```

The REST model uses Java `ChronoUnit` names such as `DAYS`, `WEEKS`, `MONTHS`, and `YEARS` where supported by the scheduler trigger implementation.

## DAILY_TIME_INTERVAL

```json
{
  "type": "DAILY_TIME_INTERVAL",
  "interval": 15,
  "unit": "MINUTES",
  "daysOfWeek": [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY"
  ],
  "startTime": "09:00:00",
  "endTime": "17:00:00"
}
```

The cadence-specific fields live inside `spec`; `timezone`, `startAt`, `endAt`, `priority`, `misfirePolicy`, and `calendarNames` belong to the outer Trigger draft.

# gRPC Cookbook

The authoritative contracts are:

```text
scheduler-contracts/src/main/proto/mcs/scheduler/v1/common.proto
scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_command.proto
scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_query.proto
```

## Create a schedule

Use different IDs from the REST Quick Start if both examples are executed against the same database.

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_command.proto \
  -d @ localhost:9091 \
  mcs.scheduler.v1.SchedulerCommandService/CreateSchedule <<'JSON'
{
  "requestId": "66666666-6666-6666-6666-666666666666",
  "namespace": "billing",
  "caller": "readme-grpc",
  "job": {
    "jobId": "77777777-7777-7777-7777-777777777777",
    "name": "grpc-invoice-dispatch",
    "description": "Created through gRPC",
    "destinationId": "44444444-4444-4444-4444-444444444444",
    "destinationVersion": "1",
    "eventType": "invoice.grpc",
    "payload": {"source": "grpc"},
    "concurrencyPolicy": "CONCURRENCY_POLICY_ALLOW",
    "recoveryPolicy": "RECOVERY_POLICY_NONE",
    "durable": true
  },
  "triggers": [
    {
      "triggerId": "88888888-8888-8888-8888-888888888888",
      "jobId": "77777777-7777-7777-7777-777777777777",
      "name": "grpc-cron",
      "description": "Every five minutes",
      "spec": {
        "cron": {
          "expression": "0 0/5 * * * ?",
          "timezone": "UTC"
        }
      },
      "priority": 5,
      "misfirePolicy": "MISFIRE_POLICY_DO_NOTHING"
    }
  ]
}
JSON
```

## Get a Job

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{"namespace":"billing","jobId":"11111111-1111-1111-1111-111111111111"}' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/GetJob
```

## List Job Triggers with pagination

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{"namespace":"billing","jobId":"11111111-1111-1111-1111-111111111111","pageSize":50,"pageToken":""}' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/ListJobTriggers
```

Pass the returned `nextPageToken` into the next request's `pageToken` until it is empty.

## Get a Trigger

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{"namespace":"billing","triggerId":"33333333-3333-3333-3333-333333333333"}' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/GetTrigger
```

## List executions

Filter by Job, Trigger, time window, state, and page when needed:

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{
    "namespace":"billing",
    "jobId":"11111111-1111-1111-1111-111111111111",
    "pageSize":50,
    "pageToken":""
  }' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/ListExecutions
```

To filter by state, use protobuf enum names such as `EXECUTION_STATE_DELIVERED`.

## Get an execution

Take an `executionId` returned by `ListExecutions`:

```bash
EXECUTION_ID='<execution-id-from-ListExecutions>'

grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d "{\"namespace\":\"billing\",\"executionId\":\"${EXECUTION_ID}\"}" \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/GetExecution
```

## Pause a Job through gRPC

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_command.proto \
  -d '{
    "requestId":"99999999-9999-9999-9999-999999999999",
    "namespace":"billing",
    "caller":"readme-grpc",
    "jobId":"11111111-1111-1111-1111-111111111111",
    "expectedRevision":"1"
  }' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerCommandService/PauseJob
```

Use the actual current revision returned by `GetJob`; `1` is only illustrative.

# Kafka Cookbook

Commands are consumed from `mcs.scheduler.commands.v1`. Use record key `<namespace>:<aggregateId>` so commands for one aggregate retain partition affinity.

Command results are published to `mcs.scheduler.command-results.v1`. Permanently rejected commands are published to `mcs.scheduler.commands.v1.DLT` with source/error metadata, including stable error codes.

## Publish CREATE_SCHEDULE

Create separate IDs if the REST/gRPC examples already exist:

```bash
KAFKA_JOB_ID=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
KAFKA_TRIGGER_ID=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
KAFKA_MESSAGE_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
KAFKA_REQUEST_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
OCCURRED_AT=$(python3 -c 'from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z"))')
KAFKA_FIRE_AT=$(python3 -c 'from datetime import datetime,timezone,timedelta; print((datetime.now(timezone.utc)+timedelta(minutes=3)).isoformat(timespec="seconds").replace("+00:00","Z"))')

cat <<JSON | docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic mcs.scheduler.commands.v1 \
  --property parse.key=true \
  --property key.separator='|'
billing:${KAFKA_JOB_ID}|{"schemaVersion":1,"messageId":"${KAFKA_MESSAGE_ID}","requestId":"${KAFKA_REQUEST_ID}","occurredAt":"${OCCURRED_AT}","producer":"readme-kafka","namespace":"billing","commandType":"CREATE_SCHEDULE","payload":{"job":{"jobId":"${KAFKA_JOB_ID}","namespace":"billing","name":"kafka-invoice-dispatch","description":"Created through Kafka","destinationId":"44444444-4444-4444-4444-444444444444","destinationVersion":1,"eventType":"invoice.kafka","payload":{"source":"kafka"},"headers":{},"concurrencyPolicy":"ALLOW","recoveryPolicy":"NONE","durable":true},"triggers":[{"triggerId":"${KAFKA_TRIGGER_ID}","jobId":"${KAFKA_JOB_ID}","namespace":"billing","name":"kafka-once","description":"Kafka one-shot","spec":{"type":"ONCE","fireAt":"${KAFKA_FIRE_AT}"},"startAt":"${KAFKA_FIRE_AT}","endAt":null,"priority":5,"timezone":"UTC","misfirePolicy":"FIRE_NOW","calendarNames":[]}]}}
JSON
```

`messageId` identifies the Kafka message; `requestId` identifies the logical scheduler command. A retry may use a new `messageId` while retaining the same `requestId` and payload.

## Consume command results

```bash
docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic mcs.scheduler.command-results.v1 \
  --from-beginning
```

Correlate the result to the submitted command by `requestId`.

## Inspect the DLT

```bash
docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic mcs.scheduler.commands.v1.DLT \
  --from-beginning \
  --property print.headers=true
```

For example, sending the same `requestId` with a changed payload is a permanent `IDEMPOTENCY_CONFLICT` and reaches the DLT after the configured retry policy.

## Consume execution events

```bash
docker compose -f docker/compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic billing.invoice-events.v1 \
  --from-beginning
```

# Java client module

The Java client is transport-neutral at the application boundary and has separate synchronous gRPC and asynchronous Kafka interfaces.

## Maven dependency

Use the same scheduler version as the deployed service:

```xml
<dependency>
  <groupId>io.k2iot.mcs</groupId>
  <artifactId>scheduler-client</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

## Synchronous gRPC client

```yaml
mcs:
  scheduler:
    client:
      enabled: true
      transport: grpc
      grpc-target: localhost:9091
      producer: billing-service
```

Inject `SchedulerClient`. The client writes the supplied UUID into protobuf `request_id`; passing `null` lets the client generate one logical request ID for the call.

```java
import com.google.protobuf.Timestamp;
import io.k2iot.mcs.scheduler.client.SchedulerClient;
import io.k2iot.mcs.scheduler.v1.ConcurrencyPolicy;
import io.k2iot.mcs.scheduler.v1.CreateScheduleRequest;
import io.k2iot.mcs.scheduler.v1.JobDraft;
import io.k2iot.mcs.scheduler.v1.MisfirePolicy;
import io.k2iot.mcs.scheduler.v1.OnceTrigger;
import io.k2iot.mcs.scheduler.v1.RecoveryPolicy;
import io.k2iot.mcs.scheduler.v1.ScheduleResponse;
import io.k2iot.mcs.scheduler.v1.TriggerDraft;
import io.k2iot.mcs.scheduler.v1.TriggerSpec;
import java.time.Instant;
import java.util.UUID;

Instant fireAt = Instant.now().plusSeconds(120);
Timestamp timestamp = Timestamp.newBuilder()
    .setSeconds(fireAt.getEpochSecond())
    .setNanos(fireAt.getNano())
    .build();

String jobId = UUID.randomUUID().toString();
String triggerId = UUID.randomUUID().toString();

CreateScheduleRequest request = CreateScheduleRequest.newBuilder()
    .setNamespace("billing")
    .setCaller("billing-service")
    .setJob(JobDraft.newBuilder()
        .setJobId(jobId)
        .setName("invoice-dispatch")
        .setDestinationId("44444444-4444-4444-4444-444444444444")
        .setDestinationVersion(1)
        .setEventType("invoice.due")
        .setConcurrencyPolicy(ConcurrencyPolicy.CONCURRENCY_POLICY_ALLOW)
        .setRecoveryPolicy(RecoveryPolicy.RECOVERY_POLICY_NONE)
        .setDurable(true)
        .build())
    .addTriggers(TriggerDraft.newBuilder()
        .setTriggerId(triggerId)
        .setJobId(jobId)
        .setName("invoice-once")
        .setSpec(TriggerSpec.newBuilder()
            .setOnce(OnceTrigger.newBuilder().setFireAt(timestamp).build())
            .build())
        .setPriority(5)
        .setMisfirePolicy(MisfirePolicy.MISFIRE_POLICY_FIRE_NOW)
        .build())
    .build();

UUID requestId = UUID.randomUUID();
ScheduleResponse response = schedulerClient.createSchedule(request, requestId);
```

The synchronous interface also exposes `pauseJob`, `resumeJob`, `deleteJob`, and `fireTrigger`.

## Asynchronous Kafka client

```yaml
mcs:
  scheduler:
    client:
      enabled: true
      transport: kafka
      kafka-command-topic: mcs.scheduler.commands.v1
      producer: billing-service
```

Inject `AsyncSchedulerClient`. Publication returns a receipt; the final command outcome remains asynchronous on the command-result topic.

```java
import io.k2iot.mcs.scheduler.client.AsyncSchedulerClient;
import io.k2iot.mcs.scheduler.client.CommandReceipt;
import java.util.UUID;

UUID requestId = UUID.randomUUID();
CommandReceipt receipt = asyncSchedulerClient.createSchedule(request, requestId);

System.out.println(receipt.requestId());
System.out.println(receipt.messageId());
System.out.println(receipt.topic());
```

Only one client transport is auto-configured per application through `mcs.scheduler.client.transport`.

# Configuration reference

`application.yml` is the authoritative source for service defaults. Important operational values are summarized below.

| Concern | Property / environment | Default / local Compose |
|---|---|---|
| REST port | `server.port` | `8080` in container; host `8081` / `8082` |
| gRPC port | `spring.grpc.server.port` | `9090` in container; host `9091` / `9092` |
| PostgreSQL URL | `SPRING_DATASOURCE_URL` | `jdbc:postgresql://postgres:5432/scheduler` in Compose |
| PostgreSQL user | `SPRING_DATASOURCE_USERNAME` | `scheduler` |
| PostgreSQL password | `SPRING_DATASOURCE_PASSWORD` | `scheduler` for local Compose only |
| Kafka bootstrap | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` in Compose; `localhost:29092` externally |
| scheduler instance | `MCS_SCHEDULER_INSTANCE_ID` | `${HOSTNAME:local}`; explicit per Compose node |
| command topic | `MCS_SCHEDULER_KAFKA_COMMAND_TOPIC` | `mcs.scheduler.commands.v1` |
| result topic | `MCS_SCHEDULER_KAFKA_COMMAND_RESULT_TOPIC` | `mcs.scheduler.command-results.v1` |
| DLT topic | `MCS_SCHEDULER_KAFKA_DLT_TOPIC` | `mcs.scheduler.commands.v1.DLT` |
| Kafka consumer group | `MCS_SCHEDULER_KAFKA_CONSUMER_GROUP` | `mcs-scheduler` |
| command partitions | `MCS_SCHEDULER_KAFKA_PARTITIONS` | `6` |
| command retry attempts | `MCS_SCHEDULER_KAFKA_RETRY_ATTEMPTS` | `2` |
| outbox enabled | `MCS_SCHEDULER_OUTBOX_ENABLED` | `true` |
| outbox batch size | `MCS_SCHEDULER_OUTBOX_BATCH_SIZE` | `100` |
| outbox poll interval | `MCS_SCHEDULER_OUTBOX_POLL_INTERVAL` | `1s` |
| outbox claim timeout | `MCS_SCHEDULER_OUTBOX_CLAIM_TIMEOUT` | `30s` |
| outbox publish timeout | `MCS_SCHEDULER_OUTBOX_PUBLISH_TIMEOUT` | `30s` |
| outbox max attempts | `MCS_SCHEDULER_OUTBOX_MAX_ATTEMPTS` | `20` |
| outbox max age | `MCS_SCHEDULER_OUTBOX_MAX_AGE` | `24h` |
| Quartz cluster check-in | `org.quartz.jobStore.clusterCheckinInterval` | `10000` ms |
| Quartz misfire threshold | `org.quartz.jobStore.misfireThreshold` | `60000` ms |

Do not copy the local Compose database password into a real environment.

# Observability

Health:

```bash
curl -fsS http://localhost:8081/actuator/health
curl -fsS http://localhost:8082/actuator/health
```

Prometheus metrics:

```bash
curl -fsS http://localhost:8081/actuator/prometheus
```

Verify both Quartz nodes in shared PostgreSQL:

```bash
docker compose -f docker/compose.yml exec -T postgres \
  psql -U scheduler -d scheduler -Atc \
  'select count(*) from quartz.qrtz_scheduler_state;'
```

A healthy local two-node topology should report `2` while both nodes are running and checked in.

Probe gRPC transport with the same query contract used in CI:

```bash
grpcurl -plaintext \
  -import-path scheduler-contracts/src/main/proto \
  -proto mcs/scheduler/v1/scheduler_query.proto \
  -d '{"namespace":"diagnostic","jobId":"00000000-0000-0000-0000-000000000001"}' \
  localhost:9091 \
  mcs.scheduler.v1.SchedulerQueryService/GetJob
```

A `JOB_NOT_FOUND` / gRPC `NOT_FOUND` response still proves that the transport and service are reachable.

# Verification and CI

Fast local formatting/unit/package verification:

```bash
./mvnw -B -ntp -DskipITs spotless:check verify
```

Full Maven verification:

```bash
./mvnw -B -ntp spotless:check verify
```

Cluster repetition:

```bash
for i in 1 2 3; do
  ./mvnw -B -ntp -pl scheduler-service -am \
    -Dfailsafe.failIfNoSpecifiedTests=false \
    -Dit.test=RestartPersistenceIT,TwoNodeQuartzClusterIT verify || exit 1
done
```

Process-kill recovery:

```bash
./mvnw -B -ntp -pl scheduler-service -am \
  -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dit.test=ProcessKillRecoveryIT verify
```

Final cross-interface acceptance:

```bash
./mvnw -B -ntp -pl scheduler-service -am \
  -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dit.test=FinalAcceptanceIT verify
```

Validate Docker Compose:

```bash
docker compose -f docker/compose.yml config
```

The GitHub Actions aggregate `validate` job requires all of these gates to succeed:

```text
format-and-unit
postgres-kafka-integration
final-acceptance
quartz-two-node-cluster
process-kill-recovery
package-image
compose-smoke
migration-compatibility
```

`migration-compatibility` rejects edits, deletes, or renames of released Flyway migrations; schema evolution must add a new versioned migration.

`compose-smoke` starts the real two-node Docker topology, verifies both REST health endpoints, verifies two Quartz scheduler-state rows, and probes the gRPC transport.

`FinalAcceptanceIT` covers REST/gRPC/Kafka creation, replay/idempotency conflicts, gRPC reads, command results, DLT handling, and registered Kafka execution delivery.

# Operations runbook

See [docs/operations-runbook.md](docs/operations-runbook.md) for PostgreSQL/Kafka outage behavior, Quartz failover interpretation, misfire diagnosis, outbox repair, rolling deployments, clock requirements, and reconciliation procedures.
