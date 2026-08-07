# MCS Scheduler Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans (recommended) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a clustered PostgreSQL-backed Quartz scheduling service that exposes equivalent REST, gRPC, and Kafka command interfaces and durably emits scheduled business events through Kafka.

**Architecture:** A Maven multi-module project separates public contracts/client code from the deployable service. The service treats Job and Trigger as separate domain resources, projects them transactionally into Quartz through Spring's `LocalDataSourceJobStore`, and uses transactional inbox/outbox tables for Kafka command ingestion and scheduled execution delivery. Quartz executes only short generic dispatcher jobs; consumer business logic remains in downstream microservices.

**Tech Stack:** Java 21, Maven, Spring Boot 4.0.x, Spring gRPC 1.0.x, Quartz 2.5.x, PostgreSQL 16+, Flyway, Spring Kafka, Protocol Buffers, Jackson, Micrometer, JUnit 5, AssertJ, Mockito, MockMvc, and Testcontainers.

## Global Constraints

- Preserve Quartz semantics: a Job defines work, a Trigger defines time, and one Job may own many Triggers.
- Do not accept arbitrary Java class names or executable code in any public contract.
- PostgreSQL domain tables are the control-plane source of truth; Quartz tables are the execution projection.
- Configure Quartz through Spring's DataSource and `LocalDataSourceJobStore`; do not configure a second Quartz-owned connection pool.
- Perform persistent Scheduler mutations inside Spring-managed transactions.
- Store only string identifiers in Quartz `JobDataMap`; store payload JSON in domain tables.
- All mutating requests require a UUID request ID and canonical-payload idempotency validation.
- Scheduled delivery is at least once through Kafka; `executionId` is the required downstream idempotency key.
- V1 outbound delivery supports only registered Kafka destinations.
- JSON payload size is limited to 64 KiB; headers are limited to 32 entries and 4 KiB total.
- The minimum repeating interval is one second.
- No code task is complete until its focused tests and the relevant module test suite pass.

---

## Planned file map

```text
pom.xml                                           Maven parent and module list
scheduler-contracts/                              Protobuf and shared wire contracts
scheduler-client/                                 Java integration facade for consumers
scheduler-service/                                Deployable Spring Boot service
scheduler-service/src/main/java/io/k2iot/mcs/scheduler/
  configuration/                                  Quartz, Kafka, Jackson, security ports
  destination/                                    Registered outbound destination model
  job/                                            Job domain, commands, persistence
  trigger/                                        Trigger domain, commands, Quartz mapping
  command/                                        Request idempotency and command results
  execution/                                      Firing identity, execution state, dispatcher
  outbox/                                         Durable Kafka publishing
  kafka/                                          Command ingress and wire mapping
  rest/                                           HTTP controllers and problem details
  grpc/                                           gRPC services and error mapping
  observability/                                  Health, metrics, reconciliation
scheduler-service/src/main/resources/db/migration Domain and Quartz migrations
scheduler-service/src/test/                       Unit, adapter, integration, cluster tests
docker/compose.yml                                Local PostgreSQL, Kafka, and two service nodes
.github/workflows/ci.yml                          Build and test pipeline
```

---

### Task 1: Bootstrap the Maven modules and executable service

**Files:**
- Create: `pom.xml`
- Create: `scheduler-contracts/pom.xml`
- Create: `scheduler-client/pom.xml`
- Create: `scheduler-service/pom.xml`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/SchedulerApplication.java`
- Create: `scheduler-service/src/main/resources/application.yml`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/SchedulerApplicationTest.java`

**Interfaces:**
- Produces: Maven modules `scheduler-contracts`, `scheduler-client`, and `scheduler-service`.
- Produces: executable class `io.k2iot.mcs.scheduler.SchedulerApplication`.
- Consumes: no project-local interfaces.

- [ ] **Step 1: Create a failing application-context test**

```java
package io.k2iot.mcs.scheduler;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.quartz.autoconfigure.QuartzAutoConfiguration,org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"
})
class SchedulerApplicationTest {

    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 2: Run the focused test and verify that the project does not compile**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerApplicationTest test
```

Expected: FAIL because the Maven project and application class do not exist.

- [ ] **Step 3: Create the parent build and module POMs**

The parent POM must:

```xml
<properties>
  <java.version>21</java.version>
  <spring-boot.version>4.0.7</spring-boot.version>
  <spring-grpc.version>1.0.3</spring-grpc.version>
</properties>
<modules>
  <module>scheduler-contracts</module>
  <module>scheduler-client</module>
  <module>scheduler-service</module>
</modules>
```

Import `spring-boot-dependencies` and `spring-grpc-dependencies` in dependency management. Configure Maven Compiler with release 21, Surefire, Failsafe, Spotless, and JaCoCo. Add Maven Wrapper pinned to a Maven version supported by Spring Boot 4.0.x.

The service module dependencies must include:

```xml
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-validation</artifactId></dependency>
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-jdbc</artifactId></dependency>
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-quartz</artifactId></dependency>
<dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
<dependency><groupId>org.springframework.grpc</groupId><artifactId>spring-grpc-spring-boot-starter</artifactId></dependency>
<dependency><groupId>org.flywaydb</groupId><artifactId>flyway-core</artifactId></dependency>
<dependency><groupId>org.flywaydb</groupId><artifactId>flyway-database-postgresql</artifactId></dependency>
<dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId><scope>runtime</scope></dependency>
```

Add test dependencies for Spring Boot Test, Spring Kafka Test, Spring gRPC Test, Mockito, Testcontainers JUnit, PostgreSQL, and Kafka.

- [ ] **Step 4: Add the application class and safe baseline configuration**

```java
package io.k2iot.mcs.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SchedulerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }
}
```

`application.yml` must expose HTTP on 8080, gRPC on 9090, Actuator health/info/prometheus, and keep Quartz schema initialization disabled because Flyway owns schema creation.

- [ ] **Step 5: Run formatting and the focused test**

Run:

```bash
./mvnw spotless:apply
./mvnw -pl scheduler-service -am -Dtest=SchedulerApplicationTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pom.xml .mvn mvnw mvnw.cmd scheduler-contracts scheduler-client scheduler-service
git commit -m "build: bootstrap scheduler modules"
```

---

### Task 2: Create PostgreSQL domain and Quartz schemas with Flyway

**Files:**
- Create: `scheduler-service/src/main/resources/db/migration/V001__create_scheduler_schema.sql`
- Create: `scheduler-service/src/main/resources/db/migration/V002__create_quartz_schema.sql`
- Create: `scheduler-service/src/main/resources/db/migration/V003__create_scheduler_tables.sql`
- Create: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/testing/PostgresIntegrationTestBase.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/configuration/DatabaseMigrationIT.java`

**Interfaces:**
- Produces schemas `scheduler` and `quartz`.
- Produces tables `scheduler.destination`, `job_definition`, `trigger_definition`, `command_request`, `inbox_message`, `execution`, `outbox_event`, and `audit_event`.
- Produces official Quartz `quartz.QRTZ_*` tables.

- [ ] **Step 1: Write a failing Testcontainers migration test**

```java
class DatabaseMigrationIT extends PostgresIntegrationTestBase {

    @Autowired JdbcTemplate jdbc;

    @Test
    void createsDomainAndQuartzTables() {
        assertThat(jdbc.queryForObject(
            "select count(*) from information_schema.tables where table_schema='scheduler'",
            Integer.class)).isGreaterThanOrEqualTo(8);
        assertThat(jdbc.queryForObject(
            "select count(*) from information_schema.tables where table_schema='quartz' and table_name like 'qrtz_%'",
            Integer.class)).isGreaterThanOrEqualTo(11);
    }
}
```

- [ ] **Step 2: Run the integration test and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=DatabaseMigrationIT test
```

Expected: FAIL because schemas and tables do not exist.

- [ ] **Step 3: Add migrations with explicit constraints and indexes**

`job_definition` must include:

```sql
job_id uuid primary key,
namespace varchar(64) not null,
name varchar(128) not null,
destination_id uuid not null,
destination_version bigint not null,
event_type varchar(255) not null,
payload jsonb not null,
headers jsonb not null default '{}'::jsonb,
concurrency_policy varchar(32) not null,
recovery_policy varchar(32) not null,
durable boolean not null,
state varchar(32) not null,
revision bigint not null,
created_at timestamptz not null,
updated_at timestamptz not null,
created_by varchar(255) not null,
updated_by varchar(255) not null
```

Create partial unique indexes that exclude deleted records for `(namespace, name)` and `(job_id, name)`. Add a unique constraint on `command_request.request_id`, `inbox_message.message_id`, normal execution `(trigger_id, scheduled_fire_time)` where `manual_fire_id is null`, and manual execution `manual_fire_id` where non-null.

Copy the PostgreSQL Quartz 2.5.x schema into `V002`, changing table references to the `quartz` schema while preserving official column types, primary keys, and indexes.

- [ ] **Step 4: Run migrations twice to verify idempotent startup**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=DatabaseMigrationIT test
./mvnw -pl scheduler-service -am -Dtest=DatabaseMigrationIT test
```

Expected: PASS both times.

- [ ] **Step 5: Commit**

```bash
git add scheduler-service/src/main/resources/db/migration scheduler-service/src/test/java/io/k2iot/mcs/scheduler/testing scheduler-service/src/test/java/io/k2iot/mcs/scheduler/configuration
git commit -m "feat: add scheduler and quartz database schemas"
```

---

### Task 3: Implement immutable Job, Trigger, Destination, and execution domain types

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/job/JobDefinition.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/job/ConcurrencyPolicy.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/job/RecoveryPolicy.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/TriggerSpec.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/OnceTriggerSpec.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/CronTriggerSpec.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/SimpleIntervalTriggerSpec.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/CalendarIntervalTriggerSpec.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/DailyTimeIntervalTriggerSpec.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/TriggerDefinition.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/TriggerValidator.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/destination/DestinationDefinition.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/execution/ExecutionIdentity.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/trigger/TriggerValidatorTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/execution/ExecutionIdentityTest.java`

**Interfaces:**
- Produces sealed interface `TriggerSpec`.
- Produces `TriggerValidator.validate(TriggerDefinition definition, Instant now)`.
- Produces `ExecutionIdentity.forScheduled(UUID triggerId, Instant scheduledFireTime)` and `forManual(UUID manualFireId)`.

- [ ] **Step 1: Write failing trigger validation tests**

```java
@Test
void rejectsCronWithoutTimezone() {
    var trigger = Fixtures.cronTrigger("0 0 8 * * ?", null);
    assertThatThrownBy(() -> validator.validate(trigger, NOW))
        .isInstanceOf(InvalidTriggerException.class)
        .hasMessageContaining("timezone");
}

@Test
void rejectsSimpleIntervalBelowOneSecond() {
    var trigger = Fixtures.simpleTrigger(Duration.ofMillis(999));
    assertThatThrownBy(() -> validator.validate(trigger, NOW))
        .isInstanceOf(InvalidTriggerException.class)
        .hasMessageContaining("one second");
}
```

- [ ] **Step 2: Write failing deterministic identity tests**

```java
@Test
void scheduledIdentityIsStableAcrossRecoveryAttempts() {
    UUID first = ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME);
    UUID second = ExecutionIdentity.forScheduled(TRIGGER_ID, FIRE_TIME);
    assertThat(second).isEqualTo(first);
}
```

- [ ] **Step 3: Run the tests and verify missing types**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=TriggerValidatorTest,ExecutionIdentityTest test
```

Expected: FAIL at compilation.

- [ ] **Step 4: Implement records, sealed trigger variants, and validation**

Use Java records for immutable values. The sealed trigger interface must be:

```java
public sealed interface TriggerSpec permits OnceTriggerSpec, CronTriggerSpec,
        SimpleIntervalTriggerSpec, CalendarIntervalTriggerSpec, DailyTimeIntervalTriggerSpec {
}
```

Use Quartz `CronExpression` to validate cron syntax. Validate timezone through `ZoneId.of`. Enforce start/end order, allowed repeat units, minimum interval, and type-specific misfire policies.

Implement deterministic UUID generation using SHA-256 over the stable string `scheduled:<triggerId>:<scheduledFireTime.toEpochMilli()>`, truncated and normalized to UUID version 5 bits. Manual fire identity returns the supplied UUID.

- [ ] **Step 5: Run focused tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=TriggerValidatorTest,ExecutionIdentityTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/{job,trigger,destination,execution} scheduler-service/src/test/java/io/k2iot/mcs/scheduler/{trigger,execution}
git commit -m "feat: add scheduler domain model and validation"
```

---

### Task 4: Define versioned gRPC and Kafka wire contracts

**Files:**
- Create: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/common.proto`
- Create: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_command.proto`
- Create: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_query.proto`
- Create: `scheduler-contracts/src/main/proto/mcs/scheduler/v1/scheduler_event.proto`
- Modify: `scheduler-contracts/pom.xml`
- Test: `scheduler-contracts/src/test/java/io/k2iot/mcs/scheduler/contracts/GeneratedContractsTest.java`

**Interfaces:**
- Produces generated `SchedulerCommandServiceGrpc` and `SchedulerQueryServiceGrpc`.
- Produces Protobuf messages for jobs, trigger `oneof`, command results, and execution events.
- Kafka JSON remains a transport envelope in the service; Protobuf messages define gRPC and client-library types.

- [ ] **Step 1: Write a failing generated-contract test**

```java
@Test
void cronTriggerRoundTripsThroughProtobuf() throws Exception {
    TriggerSpec source = TriggerSpec.newBuilder()
        .setCron(CronTrigger.newBuilder()
            .setExpression("0 0 8 * * ?")
            .setTimezone("Asia/Ho_Chi_Minh"))
        .build();

    assertThat(TriggerSpec.parseFrom(source.toByteArray())).isEqualTo(source);
}
```

- [ ] **Step 2: Run and verify generated classes are absent**

Run:

```bash
./mvnw -pl scheduler-contracts -Dtest=GeneratedContractsTest test
```

Expected: FAIL at compilation.

- [ ] **Step 3: Add protobuf definitions**

`SchedulerCommandService` must include RPCs:

```proto
rpc CreateJob(CreateJobRequest) returns (JobResponse);
rpc UpdateJob(UpdateJobRequest) returns (JobResponse);
rpc PauseJob(JobMutationRequest) returns (JobResponse);
rpc ResumeJob(JobMutationRequest) returns (JobResponse);
rpc DeleteJob(DeleteJobRequest) returns (google.protobuf.Empty);
rpc CreateTrigger(CreateTriggerRequest) returns (TriggerResponse);
rpc ReplaceTrigger(ReplaceTriggerRequest) returns (TriggerResponse);
rpc PauseTrigger(TriggerMutationRequest) returns (TriggerResponse);
rpc ResumeTrigger(TriggerMutationRequest) returns (TriggerResponse);
rpc DeleteTrigger(DeleteTriggerRequest) returns (google.protobuf.Empty);
rpc FireTriggerNow(FireTriggerRequest) returns (ExecutionResponse);
rpc CreateSchedule(CreateScheduleRequest) returns (ScheduleResponse);
```

Use `google.protobuf.Timestamp`, `Duration`, `Struct`, and a `oneof` for trigger kinds. Field numbers are never reused after release.

- [ ] **Step 4: Configure protobuf and gRPC code generation**

Configure `protobuf-maven-plugin` with the managed protobuf and grpc-java versions from the Spring gRPC BOM. Attach generated sources and include `javax.annotation-api` only if the generated compiler requires it.

- [ ] **Step 5: Run contract tests**

Run:

```bash
./mvnw -pl scheduler-contracts test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scheduler-contracts
git commit -m "feat: define scheduler grpc contracts"
```

---

### Task 5: Configure clustered Quartz and implement trigger mapping

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/configuration/QuartzConfiguration.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/quartz/QuartzKeys.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/quartz/QuartzTriggerMapper.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/quartz/ConcurrentDispatchQuartzJob.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/quartz/NonConcurrentDispatchQuartzJob.java`
- Modify: `scheduler-service/src/main/resources/application.yml`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/quartz/QuartzTriggerMapperTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/quartz/QuartzConfigurationIT.java`

**Interfaces:**
- Produces `QuartzKeys.job(UUID, String)` and `QuartzKeys.trigger(UUID, String)`.
- Produces `Trigger QuartzTriggerMapper.toQuartz(TriggerDefinition definition, JobKey jobKey)`.
- Produces generic Quartz job classes that delegate by job ID.

- [ ] **Step 1: Write failing mapper tests for every trigger variant**

```java
@Test
void mapsCalendarIntervalMonthsWithoutConvertingToSeconds() {
    Trigger trigger = mapper.toQuartz(Fixtures.everySixMonths(), JOB_KEY);
    assertThat(trigger).isInstanceOf(CalendarIntervalTrigger.class);
    CalendarIntervalTrigger calendar = (CalendarIntervalTrigger) trigger;
    assertThat(calendar.getRepeatIntervalUnit()).isEqualTo(DateBuilder.IntervalUnit.MONTH);
    assertThat(calendar.getRepeatInterval()).isEqualTo(6);
}
```

- [ ] **Step 2: Write a failing integration assertion for cluster properties**

```java
@Test
void usesPersistentClusteredSpringManagedJobStore() throws Exception {
    SchedulerMetaData metadata = scheduler.getMetaData();
    assertThat(metadata.isJobStoreSupportsPersistence()).isTrue();
    assertThat(metadata.isJobStoreClustered()).isTrue();
    assertThat(metadata.getJobStoreClass().getName())
        .contains("LocalDataSourceJobStore");
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=QuartzTriggerMapperTest,QuartzConfigurationIT test
```

Expected: FAIL because mapping and configuration are absent.

- [ ] **Step 4: Configure SchedulerFactoryBean through Spring's DataSource**

Create a `SchedulerFactoryBeanCustomizer` that sets a Spring-aware `JobFactory` and the Quartz table prefix `quartz.QRTZ_`. Do not set `org.quartz.jobStore.dataSource` or a Quartz pool.

Required properties:

```yaml
spring:
  quartz:
    job-store-type: jdbc
    jdbc:
      initialize-schema: never
    properties:
      org.quartz.scheduler.instanceName: mcs-scheduler
      org.quartz.scheduler.instanceId: AUTO
      org.quartz.scheduler.batchTriggerAcquisitionMaxCount: 10
      org.quartz.jobStore.isClustered: true
      org.quartz.jobStore.driverDelegateClass: org.quartz.impl.jdbcjobstore.PostgreSQLDelegate
      org.quartz.jobStore.tablePrefix: quartz.QRTZ_
      org.quartz.jobStore.clusterCheckinInterval: 10000
      org.quartz.jobStore.misfireThreshold: 60000
      org.quartz.jobStore.acquireTriggersWithinLock: true
      org.quartz.threadPool.threadCount: 10
```

- [ ] **Step 5: Implement the Quartz mapper**

Map each domain trigger to its matching Quartz builder and explicit misfire instruction. Set identity, job key, start/end, priority, timezone, and calendars. Never silently fall back to Quartz smart policy when the request specifies a policy.

- [ ] **Step 6: Run focused and integration tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=QuartzTriggerMapperTest,QuartzConfigurationIT test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/{configuration,quartz} scheduler-service/src/main/resources/application.yml scheduler-service/src/test/java/io/k2iot/mcs/scheduler/quartz
git commit -m "feat: configure clustered quartz and trigger mapping"
```

---

### Task 6: Implement transactional repositories and Job/Trigger command facade

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/job/JobRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/job/JdbcJobRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/TriggerRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/JdbcTriggerRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/destination/DestinationRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/CommandRequestRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/RequestFingerprint.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/SchedulerCommandFacade.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/SchedulerCommands.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/command/SchedulerCommandFacadeTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/command/SchedulerCommandFacadeIT.java`

**Interfaces:**
- Produces `SchedulerCommandFacade.createJob`, `createTrigger`, `createSchedule`, `updateJob`, `replaceTrigger`, pause/resume/delete, and manual-fire methods.
- Produces repository ports used by execution and query tasks.
- Consumes `QuartzTriggerMapper`, Spring `Scheduler`, and destination validation.

- [ ] **Step 1: Write a failing mock-based transaction orchestration test**

```java
@Test
void createTriggerPersistsDomainAndSchedulesQuartzProjection() throws Exception {
    TriggerView result = facade.createTrigger(COMMAND);

    verify(triggerRepository).insert(any(TriggerDefinition.class));
    verify(quartzScheduler).scheduleJob(any(Trigger.class));
    assertThat(result.jobId()).isEqualTo(COMMAND.jobId());
}
```

- [ ] **Step 2: Write a failing Testcontainers atomicity test**

Inject a `Scheduler` mock that throws after the domain insert and assert no `trigger_definition` row remains after the transaction rolls back.

```java
assertThat(jdbc.queryForObject(
    "select count(*) from scheduler.trigger_definition where trigger_id=?",
    Integer.class, triggerId)).isZero();
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerCommandFacadeTest,SchedulerCommandFacadeIT test
```

Expected: FAIL.

- [ ] **Step 4: Implement JDBC repositories with explicit SQL**

Use `JdbcClient` or `NamedParameterJdbcTemplate`; do not add JPA. Map JSONB through Jackson and PostgreSQL `PGobject`. Every update includes `where revision = :expectedRevision` and increments revision. Zero updated rows maps to `REVISION_CONFLICT` after distinguishing not-found from stale revision.

- [ ] **Step 5: Implement request idempotency**

Canonicalize command JSON by sorting object properties, hash with SHA-256, and insert `command_request(request_id, request_hash, state, response_json)`. On duplicate request ID:

- same hash and completed state returns stored response;
- same hash and in-progress state returns a retriable conflict;
- different hash returns `IDEMPOTENCY_CONFLICT`.

- [ ] **Step 6: Implement facade operations inside `@Transactional` methods**

Create JobDetails with only:

```java
JobDataMap data = new JobDataMap(Map.of(
    "jobId", definition.jobId().toString(),
    "namespace", definition.namespace(),
    "revision", Long.toString(definition.revision())
));
```

Select `ConcurrentDispatchQuartzJob` or `NonConcurrentDispatchQuartzJob` from policy. Set durable and request-recovery flags. Use `scheduler.addJob`, `scheduleJob`, `rescheduleJob`, `pauseJob`, `pauseTrigger`, and deletion APIs; never write Quartz tables directly.

- [ ] **Step 7: Verify one Job can own multiple Triggers**

Add an integration test creating one durable job with a cron trigger and a one-shot trigger, then assert Quartz returns two trigger keys for the same job key and both domain rows exist.

- [ ] **Step 8: Run tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerCommandFacadeTest,SchedulerCommandFacadeIT test
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/{job,trigger,destination,command} scheduler-service/src/test/java/io/k2iot/mcs/scheduler/command
git commit -m "feat: add transactional scheduler command facade"
```

---

### Task 7: Expose the REST control plane

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/JobController.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/TriggerController.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/ScheduleController.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/ExecutionController.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/RestModels.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/RestCommandMapper.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest/ProblemDetailsAdvice.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/rest/ScheduleControllerTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/rest/TriggerControllerTest.java`

**Interfaces:**
- Consumes `SchedulerCommandFacade` and query ports.
- Produces `/api/v1/jobs`, `/triggers`, `/schedules`, and `/executions` endpoints.
- Produces RFC 9457 problem responses with stable domain error codes.

- [ ] **Step 1: Write failing MockMvc tests for the convenience API**

```java
mockMvc.perform(post("/api/v1/schedules")
        .header("Idempotency-Key", REQUEST_ID)
        .contentType(APPLICATION_JSON)
        .content(validCreateScheduleJson()))
    .andExpect(status().isCreated())
    .andExpect(header().exists("ETag"))
    .andExpect(jsonPath("$.job.jobId").exists())
    .andExpect(jsonPath("$.triggers.length()").value(1));
```

- [ ] **Step 2: Write failing validation and revision tests**

Cover missing idempotency key, invalid cron, payload larger than 64 KiB, unknown destination, and stale `If-Match` returning HTTP 412 with code `REVISION_CONFLICT`.

- [ ] **Step 3: Run tests and verify 404/compilation failures**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=ScheduleControllerTest,TriggerControllerTest test
```

Expected: FAIL.

- [ ] **Step 4: Implement DTOs as sealed request models**

Use Jackson polymorphism on `type`. Example:

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CronTriggerRequest.class, name = "CRON"),
    @JsonSubTypes.Type(value = OnceTriggerRequest.class, name = "ONCE")
})
public sealed interface TriggerRequest permits CronTriggerRequest, OnceTriggerRequest,
        SimpleIntervalTriggerRequest, CalendarIntervalTriggerRequest, DailyTimeIntervalTriggerRequest {
}
```

- [ ] **Step 5: Implement controllers and problem mapping**

Return 201 with `Location` for create operations, 200 for state transitions, 204 for deletes, and ETag from revision. Never expose Quartz class names, table names, or serialized JobDataMap content.

- [ ] **Step 6: Run REST tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest='*ControllerTest' test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/rest scheduler-service/src/test/java/io/k2iot/mcs/scheduler/rest
git commit -m "feat: expose scheduler rest api"
```

---

### Task 8: Expose equivalent gRPC command and query services

**Files:**
- Modify: `scheduler-service/pom.xml`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/grpc/SchedulerCommandGrpcService.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/grpc/SchedulerQueryGrpcService.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/grpc/GrpcCommandMapper.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/grpc/GrpcErrorMapper.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/grpc/SchedulerCommandGrpcServiceTest.java`

**Interfaces:**
- Consumes generated contracts and `SchedulerCommandFacade`.
- Produces gRPC services on the Spring gRPC server.
- Produces stable mapping from domain errors to gRPC status and error details.

- [ ] **Step 1: Write a failing in-process gRPC test**

```java
@Test
void createsScheduleWithSameSemanticsAsRest() {
    ScheduleResponse response = stub.createSchedule(validGrpcRequest());
    assertThat(response.getJob().getNamespace()).isEqualTo("billing");
    assertThat(response.getTriggersCount()).isEqualTo(1);
}
```

- [ ] **Step 2: Write a stale revision status test**

Assert that `REVISION_CONFLICT` maps to `Status.Code.ABORTED` and includes a structured error detail with the scheduler domain code.

- [ ] **Step 3: Run and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerCommandGrpcServiceTest test
```

Expected: FAIL.

- [ ] **Step 4: Implement mappers and services**

Map Protobuf timestamps and durations without using the JVM default timezone. Extract request ID and caller identity from request fields plus configured gRPC metadata interceptor. Delegate every RPC to the same facade methods used by REST.

- [ ] **Step 5: Run tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerCommandGrpcServiceTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scheduler-service/pom.xml scheduler-service/src/main/java/io/k2iot/mcs/scheduler/grpc scheduler-service/src/test/java/io/k2iot/mcs/scheduler/grpc
git commit -m "feat: expose scheduler grpc api"
```

---

### Task 9: Add Kafka command ingestion with inbox deduplication

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/kafka/SchedulerCommandEnvelope.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/kafka/KafkaCommandMapper.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/kafka/SchedulerCommandListener.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/kafka/KafkaTopicConfiguration.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/InboxRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/JdbcInboxRepository.java`
- Modify: `scheduler-service/src/main/resources/application.yml`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/kafka/SchedulerCommandListenerTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/kafka/SchedulerCommandKafkaIT.java`

**Interfaces:**
- Consumes topic `mcs.scheduler.commands.v1`.
- Produces a durable command-result outbox record.
- Consumes the same `SchedulerCommandFacade` as REST and gRPC.

- [ ] **Step 1: Write a failing listener unit test**

```java
@Test
void duplicateMessageIdDoesNotExecuteCommandTwice() {
    listener.onMessage(RECORD);
    listener.onMessage(RECORD);
    verify(commandFacade, times(1)).createSchedule(any());
}
```

- [ ] **Step 2: Write a failing PostgreSQL plus Kafka integration test**

Publish the same command twice, wait for processing, and assert one inbox row, one job, and one trigger.

- [ ] **Step 3: Run and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerCommandListenerTest,SchedulerCommandKafkaIT test
```

Expected: FAIL.

- [ ] **Step 4: Implement the versioned envelope and mapper**

Reject unsupported `schemaVersion`, malformed UUIDs, namespace mismatch, unknown command type, and payloads that cannot map to the command model. Set the Kafka key to `<namespace>:<aggregateId-or-requestId>`.

- [ ] **Step 5: Implement inbox transaction semantics**

The listener transaction order is:

```text
insert inbox message -> map/execute command -> insert command-result outbox -> mark inbox completed -> commit DB -> commit Kafka offset
```

Use a bounded retry policy and publish non-retriable validation failures to `mcs.scheduler.commands.v1.DLT`. Include original topic, partition, offset, message ID, request ID, and stable domain error code in DLT headers.

- [ ] **Step 6: Run tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=SchedulerCommandListenerTest,SchedulerCommandKafkaIT test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/{kafka,command} scheduler-service/src/main/resources/application.yml scheduler-service/src/test/java/io/k2iot/mcs/scheduler/kafka
git commit -m "feat: ingest scheduler commands from kafka"
```

---

### Task 10: Record Quartz firings and create execution outbox events atomically

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/execution/ExecutionRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/execution/JdbcExecutionRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/execution/ScheduledExecutionService.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/execution/ExecutionEventFactory.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox/OutboxRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox/JdbcOutboxRepository.java`
- Modify: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/quartz/ConcurrentDispatchQuartzJob.java`
- Modify: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/quartz/NonConcurrentDispatchQuartzJob.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/execution/ScheduledExecutionServiceTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/execution/ScheduledExecutionIT.java`

**Interfaces:**
- Produces `ScheduledExecutionService.record(JobExecutionContext context)`.
- Produces one logical execution and one outbox event per scheduled occurrence.
- Consumes job, trigger, destination, execution, and outbox repositories.

- [ ] **Step 1: Write a failing duplicate-recovery unit test**

```java
@Test
void repeatedRecoveryCreatesOneLogicalExecution() {
    service.record(contextFor(TRIGGER_ID, FIRE_TIME, false));
    service.record(contextFor(TRIGGER_ID, FIRE_TIME, true));

    verify(executionRepository, times(2)).insertIfAbsent(any());
    verify(outboxRepository, times(1)).insert(any());
}
```

The repository result controls whether the outbox insert occurs.

- [ ] **Step 2: Write a failing database atomicity test**

Force outbox insertion to violate a constraint and assert the execution insert rolls back.

- [ ] **Step 3: Run and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=ScheduledExecutionServiceTest,ScheduledExecutionIT test
```

Expected: FAIL.

- [ ] **Step 4: Implement scheduled and manual execution identity**

For normal Quartz firing, derive identity from `triggerId` and `context.getScheduledFireTime().toInstant()`. For a manual fire, the command creates a one-shot volatile trigger containing `manualFireId` as a string JobDataMap entry; identity uses that UUID.

- [ ] **Step 5: Implement snapshot and suppression behavior**

Within one transaction:

1. Load active job, trigger, and bound destination version.
2. If the definition is deleted or paused because of drift, insert a `SUPPRESSED` execution without outbox.
3. Insert execution with `ON CONFLICT DO NOTHING`.
4. Only when inserted, create the Kafka execution event and outbox row.
5. Return successfully for duplicate recovery attempts.

- [ ] **Step 6: Make both Quartz classes delegate only to the service**

```java
@Override
protected void executeInternal(JobExecutionContext context) {
    scheduledExecutionService.record(context);
}
```

Do not publish to Kafka directly from the Quartz thread.

- [ ] **Step 7: Run tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=ScheduledExecutionServiceTest,ScheduledExecutionIT test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/{execution,outbox,quartz} scheduler-service/src/test/java/io/k2iot/mcs/scheduler/execution
git commit -m "feat: record durable scheduled executions"
```

---

### Task 11: Publish outbox events to Kafka with retry and delivery state

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox/OutboxPublisher.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox/OutboxClaimRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox/JdbcOutboxClaimRepository.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox/OutboxProperties.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/outbox/OutboxPublisherTest.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/outbox/OutboxPublisherKafkaIT.java`

**Interfaces:**
- Consumes pending `scheduler.outbox_event` rows using `FOR UPDATE SKIP LOCKED`.
- Produces Kafka command-result and execution events.
- Updates execution delivery state after broker acknowledgment.

- [ ] **Step 1: Write a failing claim-concurrency test**

Start two publisher workers and assert a pending outbox row is returned to only one claimant.

- [ ] **Step 2: Write a failing publish integration test**

Create an execution outbox row, invoke one publisher cycle, consume the event from Kafka, and assert the database outbox state is `PUBLISHED` and execution state is `DELIVERED`.

- [ ] **Step 3: Run and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=OutboxPublisherTest,OutboxPublisherKafkaIT test
```

Expected: FAIL.

- [ ] **Step 4: Implement short polling with safe claiming**

Use a configurable batch size, initially 100. Claim rows in a short database transaction with `FOR UPDATE SKIP LOCKED`, move them to `IN_PROGRESS`, increment attempt count, and set a claim timeout. Publish outside the claim transaction, then mark success or reschedule failure.

Retry delays are 1 second, 5 seconds, 30 seconds, 2 minutes, and 10 minutes; after 20 attempts or 24 hours, mark `DEAD` and emit a metric. Keep the original event ID and execution ID across retries.

- [ ] **Step 5: Configure Kafka producer durability**

Require idempotent producer, `acks=all`, bounded delivery timeout, and a stable client ID containing the scheduler instance ID. Event key defaults to execution ID unless the registered destination key expression resolves another approved stable key.

- [ ] **Step 6: Run tests**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=OutboxPublisherTest,OutboxPublisherKafkaIT test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/outbox scheduler-service/src/test/java/io/k2iot/mcs/scheduler/outbox
git commit -m "feat: publish scheduler outbox events"
```

---

### Task 12: Complete lifecycle queries, pause semantics, audit, and reconciliation

**Files:**
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/job/JobQueryService.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/trigger/TriggerQueryService.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/execution/ExecutionQueryService.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/observability/QuartzReconciler.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/observability/SchedulerHealthIndicator.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/observability/SchedulerMetrics.java`
- Create: `scheduler-service/src/main/java/io/k2iot/mcs/scheduler/command/AuditRepository.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/command/PauseLifecycleIT.java`
- Test: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/observability/QuartzReconcilerTest.java`

**Interfaces:**
- Produces paged query services used by REST/gRPC.
- Produces report-only reconciliation and explicit idempotent repair.
- Produces Actuator health and Micrometer metrics.

- [ ] **Step 1: Write a failing pause-reason integration test**

Create two triggers, pause one individually, pause the job, resume the job, and assert only the trigger paused by the job resumes while the individually paused trigger remains paused.

- [ ] **Step 2: Write a failing drift-report test**

Delete a Quartz trigger through the Scheduler test fixture while keeping its domain row, run reconciliation, and assert one `MISSING_QUARTZ_TRIGGER` finding without automatic mutation.

- [ ] **Step 3: Run and verify failure**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=PauseLifecycleIT,QuartzReconcilerTest test
```

Expected: FAIL.

- [ ] **Step 4: Implement lifecycle state and audit transactions**

Store `pause_reason` on trigger rows. Every mutation writes an audit event containing request ID, caller, aggregate ID, old revision/state, new revision/state, timestamp, and safe metadata without full payload content.

- [ ] **Step 5: Implement paged queries**

Use keyset pagination ordered by `(created_at, id)` for jobs/triggers and `(scheduled_fire_time, execution_id)` for executions. Reject unbounded queries and cap page size at 200.

- [ ] **Step 6: Implement report-only reconciliation and explicit repair**

The default scheduled scan records findings and metrics. `repairJob(jobId, expectedRevision)` reconstructs the JobDetail and all active triggers from domain definitions in one transaction. It never deletes an unknown Quartz object automatically.

- [ ] **Step 7: Add metrics and health**

Health is DOWN only when the local scheduler cannot start, PostgreSQL is unavailable, or required Quartz metadata cannot be read. Kafka/outbox backlog is a separate health detail and metric rather than immediately stopping trigger acquisition.

- [ ] **Step 8: Run tests and service module suite**

Run:

```bash
./mvnw -pl scheduler-service -am test
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add scheduler-service/src/main/java/io/k2iot/mcs/scheduler/{job,trigger,execution,observability,command} scheduler-service/src/test/java/io/k2iot/mcs/scheduler/{command,observability}
git commit -m "feat: add scheduler lifecycle queries and reconciliation"
```

---

### Task 13: Build the Java client integration module

**Files:**
- Modify: `scheduler-client/pom.xml`
- Create: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/SchedulerClient.java`
- Create: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/GrpcSchedulerClient.java`
- Create: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/KafkaSchedulerCommandPublisher.java`
- Create: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/SchedulerClientProperties.java`
- Create: `scheduler-client/src/main/java/io/k2iot/mcs/scheduler/client/SchedulerClientAutoConfiguration.java`
- Create: `scheduler-client/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `scheduler-client/src/test/java/io/k2iot/mcs/scheduler/client/SchedulerClientAutoConfigurationTest.java`
- Test: `scheduler-client/src/test/java/io/k2iot/mcs/scheduler/client/GrpcSchedulerClientTest.java`

**Interfaces:**
- Produces a narrow Java API for create schedule, pause/resume/delete, and manual fire.
- Produces optional synchronous gRPC and asynchronous Kafka implementations.
- Consumes only `scheduler-contracts`, Spring gRPC client, and Spring Kafka; it does not depend on `scheduler-service`.

- [ ] **Step 1: Write a failing auto-configuration test**

```java
new ApplicationContextRunner()
    .withConfiguration(AutoConfigurations.of(SchedulerClientAutoConfiguration.class))
    .withPropertyValues("mcs.scheduler.client.transport=grpc")
    .run(context -> assertThat(context).hasSingleBean(SchedulerClient.class));
```

- [ ] **Step 2: Write a failing request-ID behavior test**

Assert that a supplied request ID is preserved and a missing request ID is generated once per logical client call, not once per retry attempt.

- [ ] **Step 3: Run and verify failure**

Run:

```bash
./mvnw -pl scheduler-client -am test
```

Expected: FAIL.

- [ ] **Step 4: Implement transport-neutral client methods**

```java
public interface SchedulerClient {
    ScheduleResponse createSchedule(CreateScheduleRequest request, UUID requestId);
    JobResponse pauseJob(UUID jobId, String namespace, long expectedRevision, UUID requestId);
    JobResponse resumeJob(UUID jobId, String namespace, long expectedRevision, UUID requestId);
    ExecutionResponse fireTrigger(UUID triggerId, String namespace, UUID manualFireId, UUID requestId);
}
```

The Kafka implementation returns a command receipt rather than pretending to return a synchronous final result; expose it through a separate `AsyncSchedulerClient` interface to keep semantics honest.

- [ ] **Step 5: Implement Spring Boot auto-configuration**

Enable only when `mcs.scheduler.client.enabled=true`. Require explicit target address or Kafka topic configuration. Do not create both transports unless requested.

- [ ] **Step 6: Run client tests**

Run:

```bash
./mvnw -pl scheduler-client -am test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler-client
git commit -m "feat: add scheduler java client"
```

---

### Task 14: Prove persistence and normal two-node cluster behavior

**Files:**
- Create: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/cluster/TwoNodeQuartzClusterIT.java`
- Create: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/cluster/RestartPersistenceIT.java`
- Create: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/testing/ClusterTestApplication.java`

**Interfaces:**
- Consumes one PostgreSQL and one Kafka Testcontainer.
- Starts two independent Spring application contexts with unique Quartz instance IDs.
- Proves persistent schedules and one logical execution per occurrence.

- [ ] **Step 1: Write a failing restart persistence test**

Start node A, create a future one-shot schedule, stop node A before due time, start node B against the same PostgreSQL container, and assert the event is emitted after the due time.

- [ ] **Step 2: Write a failing two-node duplicate test**

Create 100 one-shot schedules spread across a short test window. Keep both nodes running. Assert exactly 100 distinct execution rows and 100 Kafka execution IDs, with no duplicate `(triggerId, scheduledFireTime)`.

- [ ] **Step 3: Run and observe failure before cluster fixture exists**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=RestartPersistenceIT,TwoNodeQuartzClusterIT test
```

Expected: FAIL.

- [ ] **Step 4: Implement isolated application contexts**

Set unique `spring.application.name`, server port 0, gRPC port 0, and Quartz `instanceId=AUTO` for each context. Share JDBC and Kafka endpoints. Capture each node's Quartz instance ID and include it in execution test metadata.

- [ ] **Step 5: Stabilize time-based assertions**

Use Awaitility with explicit maximum waits derived from the test cluster check-in and misfire settings. Do not use fixed sleeps as the primary assertion mechanism.

- [ ] **Step 6: Run cluster tests repeatedly**

Run:

```bash
for i in 1 2 3; do ./mvnw -pl scheduler-service -am -Dtest=RestartPersistenceIT,TwoNodeQuartzClusterIT test || exit 1; done
```

Expected: three consecutive PASS runs.

- [ ] **Step 7: Commit**

```bash
git add scheduler-service/src/test/java/io/k2iot/mcs/scheduler/{cluster,testing}
git commit -m "test: verify scheduler persistence and cluster execution"
```

---

### Task 15: Add process-kill recovery test, Docker deployment, and CI

**Files:**
- Create: `scheduler-service/src/test/java/io/k2iot/mcs/scheduler/cluster/ProcessKillRecoveryIT.java`
- Create: `scheduler-service/src/test/resources/cluster/Dockerfile.test`
- Create: `docker/compose.yml`
- Create: `Dockerfile`
- Create: `.github/workflows/ci.yml`
- Create: `README.md`
- Create: `docs/runbooks/quartz-cluster-operations.md`

**Interfaces:**
- Produces a production-like two-node local deployment.
- Proves recovery after killing the node that acquired a recovery-enabled firing.
- Produces CI gates for unit, integration, cluster, formatting, and coverage.

- [ ] **Step 1: Write a failing process-kill recovery test**

The test must:

1. Build the service image.
2. Start PostgreSQL, Kafka, scheduler node A, and scheduler node B as Testcontainers.
3. Create a recovery-enabled job whose test-only execution hook writes an `acquired` marker and blocks before the execution transaction.
4. Detect which container acquired the firing.
5. Kill that container without graceful shutdown.
6. Wait beyond the shortened test cluster check-in interval.
7. Assert the surviving node records one logical execution and Kafka receives one execution ID.

- [ ] **Step 2: Run and verify failure before deployment assets exist**

Run:

```bash
./mvnw -pl scheduler-service -am -Dtest=ProcessKillRecoveryIT verify
```

Expected: FAIL.

- [ ] **Step 3: Create the production Dockerfile and local compose topology**

Compose services:

- PostgreSQL 16 with persistent volume and healthcheck;
- Kafka in KRaft mode with healthcheck;
- scheduler-node-1 and scheduler-node-2 using identical scheduler name and shared database;
- optional Kafka console tooling only under a `tools` profile.

Do not publish the same gRPC or HTTP host ports for both nodes; expose them through separate local ports or an optional reverse proxy.

- [ ] **Step 4: Implement the containerized recovery fixture**

Use a test profile with cluster check-in of 2 seconds and misfire threshold of 3 seconds to keep the test bounded. The production defaults remain 10 and 60 seconds. The test hook is package-private/test-profile-only and cannot be enabled in normal profiles.

- [ ] **Step 5: Add CI workflow**

Jobs:

```text
format-and-unit
postgres-kafka-integration
quartz-two-node-cluster
process-kill-recovery
package-image
```

Cache Maven dependencies, upload Surefire/Failsafe reports on failure, and require all jobs before merge. Run cluster jobs on Linux with Docker available.

- [ ] **Step 6: Write README and operations runbook**

README must contain:

- architecture summary;
- Job versus Trigger examples;
- REST, gRPC, and Kafka create-schedule examples;
- local compose commands;
- test commands;
- client-module usage.

The runbook must contain:

- PostgreSQL and Kafka outage behavior;
- Quartz node check-in and failover interpretation;
- misfire backlog diagnosis;
- outbox backlog repair;
- safe rolling deployment;
- clock synchronization requirement;
- prohibition against direct `QRTZ_*` mutation;
- reconciliation report and explicit repair commands.

- [ ] **Step 7: Run the complete verification suite**

Run:

```bash
./mvnw spotless:check verify

docker compose -f docker/compose.yml config
```

Expected: all unit, integration, cluster, process-kill, formatting, and packaging checks PASS; Compose configuration is valid.

- [ ] **Step 8: Commit**

```bash
git add Dockerfile docker .github README.md docs/runbooks scheduler-service/src/test
git commit -m "test: add scheduler failover verification and deployment"
```

---

## Final verification checklist

- [ ] Run `./mvnw spotless:check verify` from a clean checkout.
- [ ] Run `docker compose -f docker/compose.yml up --build -d` and verify both nodes report clustered persistent Quartz health.
- [ ] Create one job with two triggers through REST and read it through gRPC.
- [ ] Create an equivalent schedule through Kafka and consume its command result.
- [ ] Confirm a scheduled execution event reaches the registered Kafka destination.
- [ ] Replay the same REST, gRPC, and Kafka request IDs and confirm no duplicate definitions.
- [ ] Reuse a request ID with a changed payload and confirm `IDEMPOTENCY_CONFLICT`.
- [ ] Kill one scheduler node during a recovery-enabled firing and confirm one logical execution.
- [ ] Confirm no public response or log contains secrets or full payload content by default.
- [ ] Review Flyway migration compatibility and ensure no migration edits an already released migration file.

## Plan self-review results

- Every design requirement has a corresponding implementation or verification task.
- REST, gRPC, and Kafka use one command facade rather than separate behavior.
- Job and Trigger remain separate first-class resources while `CreateSchedule` is only a transactional convenience operation.
- Spring transaction participation, deterministic execution identity, inbox/outbox, and cluster recovery are explicitly tested.
- No task requires arbitrary Java job classes, direct Quartz-table writes, unrestricted Kafka topics, or business execution inside Quartz threads.
- File paths, public method names, topic names, table names, test commands, and commit boundaries are defined without implementation placeholders.
