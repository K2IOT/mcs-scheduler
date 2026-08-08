# MCS Scheduler Operations Runbook

## PostgreSQL outage

Quartz JDBC JobStore and scheduler domain state both depend on PostgreSQL. During a database outage, do not attempt to move scheduling ownership into application memory and do not modify `quartz.QRTZ_*` manually. Keep scheduler nodes running only if the database failure is transient and backpressure is acceptable; otherwise stop command ingress before repeatedly restarting nodes.

After PostgreSQL recovers:

1. verify `/actuator/health` on every scheduler node;
2. verify rows reappear in `quartz.qrtz_scheduler_state` for each live instance;
3. inspect the misfire backlog before increasing traffic;
4. run report-only Quartz reconciliation for recently mutated jobs;
5. repair drift only through the scheduler reconciliation path with the expected domain revision.

## Kafka outage

Domain commands received through REST/gRPC can still commit scheduler state while Kafka delivery is unavailable. Execution and command-result events remain in the durable outbox and are retried by the publisher. Kafka command ingress is unavailable until brokers recover.

Monitor unpublished `scheduler.outbox_event` rows, publish attempts, last error, and the scheduler outbox metrics. Do not delete unpublished rows to clear the queue. After Kafka recovery, allow the publisher to drain the backlog and confirm published timestamps advance.

## Quartz cluster check-in and failover

Production defaults are a 10-second Quartz cluster check-in and 60-second misfire threshold. A missing scheduler-state heartbeat does not immediately mean the node has failed; interpret it against the configured check-in interval and database health.

Recovery-enabled JobDetails use Quartz `requestRecovery`. When a process dies after acquiring a firing and before the scheduler execution transaction completes, a surviving cluster node recovers the firing after Quartz detects the failed instance. The execution identity remains derived from `(triggerId, scheduledFireTime)`, so repeated recovery attempts converge on one logical execution.

The Task 15 acceptance profile deliberately shortens check-in to 2 seconds and misfire threshold to 3 seconds. Those values are test-only and must not replace production defaults.

## Misfire backlog diagnosis

When delayed triggers accumulate:

1. confirm database latency and lock pressure first;
2. inspect live node count and Quartz scheduler-state check-ins;
3. inspect trigger next-fire times and configured misfire policies;
4. check application thread-pool saturation and long-running callbacks;
5. confirm all hosts use synchronized clocks;
6. let each trigger's explicit misfire policy decide catch-up behavior rather than rewriting Quartz rows.

A large backlog combined with `FIRE_NOW`/catch-up policies can create an intentional burst. Scale consumers and Kafka capacity before forcing additional repair operations.

## Outbox backlog repair

Treat the database outbox as the delivery source of truth. For a backlog:

1. identify whether Kafka, serialization, destination configuration, or repeated poison records are responsible;
2. inspect `publish_attempts`, `last_error`, age, and destination health;
3. repair the underlying dependency/configuration;
4. allow normal claiming/retry to resume;
5. verify events receive `published_at` and the backlog returns to steady state.

Never mark events published or delete them directly merely to reduce the queue length. Any administrative replay/repair must preserve execution/event identity and idempotency.

## Safe rolling deployment

Run at least two scheduler nodes against the same PostgreSQL database and identical Quartz scheduler name.

1. confirm all nodes are healthy and the outbox is stable;
2. deploy one node at a time;
3. wait for the replacement node to appear in Quartz scheduler state and become healthy;
4. confirm trigger execution and Kafka delivery remain normal;
5. continue with the next node;
6. after the rollout, run reconciliation on representative/recently changed jobs.

Do not terminate all nodes simultaneously during a normal rollout. Use process termination only for deliberate disaster/failover drills such as `ProcessKillRecoveryIT`.

## Clock synchronization

All scheduler hosts must use synchronized system time (for example NTP/chrony managed by the platform). Scheduled-fire identity, misfire interpretation, leases/check-ins, and operational timelines depend on consistent clocks. Investigate clock skew before treating apparent early/late fires as Quartz defects.

## Never mutate `QRTZ_*` directly

The `quartz.QRTZ_*` tables are owned by Quartz. Direct SQL updates/deletes can violate Quartz locking, clustering, trigger state, and recovery invariants. Domain state belongs in `scheduler.*`; projection drift is handled by reconciliation and explicit repair.

Read-only SQL may be used for diagnosis, but write operations must go through application commands or the reconciler.

## Reconciliation report and explicit repair

`QuartzReconciler.reconcileJob(jobId)` is report-only. It compares the current domain Job/Triggers with Quartz and reports missing Quartz JobDetails or triggers without mutating state.

`QuartzReconciler.repairJob(jobId, expectedRevision)` is the explicit idempotent repair operation. Always obtain the current domain revision first and pass it as `expectedRevision`; a stale revision fails with `REVISION_CONFLICT`. The repair reprojects the Job and all non-deleted Triggers through `SchedulerProjectionPort` rather than touching Quartz tables.

For operator tooling, expose these two operations through a controlled authenticated admin surface or maintenance command that calls the reconciler directly. The operational sequence is always:

```text
reconcileJob(jobId)
  -> review findings
  -> read current domain revision
  -> repairJob(jobId, expectedRevision)
  -> reconcileJob(jobId) again
  -> expect no findings
```

Do not automate repair immediately on every report: reconciliation is intentionally report-first so operators can distinguish real drift from an active incident.

## Verification commands

```bash
./mvnw spotless:check verify
./mvnw -pl scheduler-service -am -Dit.test=ProcessKillRecoveryIT verify
docker compose -f docker/compose.yml config
```

For normal cluster stability, run the two-node tests three consecutive times:

```bash
for i in 1 2 3; do
  ./mvnw -pl scheduler-service -am \
    -Dit.test=RestartPersistenceIT,TwoNodeQuartzClusterIT verify || exit 1
done
```
