CREATE TABLE scheduler.destination (
    destination_id uuid NOT NULL,
    version bigint NOT NULL,
    namespace varchar(120) NOT NULL,
    type varchar(32) NOT NULL,
    topic varchar(249) NOT NULL,
    key_expression varchar(512),
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(120) NOT NULL DEFAULT 'system',
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by varchar(120) NOT NULL DEFAULT 'system',
    CONSTRAINT destination_pkey PRIMARY KEY (destination_id, version),
    CONSTRAINT ck_destination_version_positive CHECK (version > 0),
    CONSTRAINT ck_destination_headers_object CHECK (jsonb_typeof(headers) = 'object')
);

CREATE UNIQUE INDEX ux_destination_namespace_type_topic_version
    ON scheduler.destination (namespace, type, topic, version);
CREATE INDEX ix_destination_namespace_enabled
    ON scheduler.destination (namespace, enabled);

CREATE TABLE scheduler.job_definition (
    job_id uuid NOT NULL,
    namespace varchar(120) NOT NULL,
    name varchar(200) NOT NULL,
    description varchar(1000),
    destination_id uuid NOT NULL,
    destination_version bigint NOT NULL,
    event_type varchar(250) NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    concurrency_policy varchar(32) NOT NULL DEFAULT 'ALLOW',
    recovery_policy varchar(32) NOT NULL DEFAULT 'NONE',
    state varchar(32) NOT NULL DEFAULT 'ACTIVE',
    revision bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(120) NOT NULL DEFAULT 'system',
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by varchar(120) NOT NULL DEFAULT 'system',
    deleted_at timestamptz,
    deleted_by varchar(120),
    CONSTRAINT job_definition_pkey PRIMARY KEY (job_id),
    CONSTRAINT job_definition_destination_fk
        FOREIGN KEY (destination_id, destination_version)
        REFERENCES scheduler.destination (destination_id, version),
    CONSTRAINT ck_job_definition_revision_positive CHECK (revision > 0),
    CONSTRAINT ck_job_definition_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_job_definition_headers_object CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT ck_job_definition_state CHECK (state IN ('ACTIVE', 'PAUSED', 'DISABLED', 'DELETED')),
    CONSTRAINT ck_job_definition_delete_metadata CHECK (
        (state = 'DELETED' AND deleted_at IS NOT NULL)
        OR (state <> 'DELETED' AND deleted_at IS NULL AND deleted_by IS NULL)
    )
);

CREATE UNIQUE INDEX ux_job_definition_namespace_name_active
    ON scheduler.job_definition (namespace, name)
    WHERE state <> 'DELETED';
CREATE UNIQUE INDEX ux_job_definition_id_name_active
    ON scheduler.job_definition (job_id, name)
    WHERE state <> 'DELETED';
CREATE INDEX ix_job_definition_destination
    ON scheduler.job_definition (destination_id, destination_version);
CREATE INDEX ix_job_definition_namespace_state
    ON scheduler.job_definition (namespace, state);

CREATE TABLE scheduler.trigger_definition (
    trigger_id uuid NOT NULL,
    job_id uuid NOT NULL,
    namespace varchar(120) NOT NULL,
    name varchar(200) NOT NULL,
    description varchar(1000),
    type varchar(32) NOT NULL,
    spec jsonb NOT NULL,
    start_at timestamptz,
    end_at timestamptz,
    priority integer NOT NULL DEFAULT 5,
    timezone varchar(80),
    misfire_policy varchar(32) NOT NULL DEFAULT 'SMART_POLICY',
    calendar_names jsonb NOT NULL DEFAULT '[]'::jsonb,
    state varchar(32) NOT NULL DEFAULT 'ACTIVE',
    revision bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by varchar(120) NOT NULL DEFAULT 'system',
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by varchar(120) NOT NULL DEFAULT 'system',
    deleted_at timestamptz,
    deleted_by varchar(120),
    CONSTRAINT trigger_definition_pkey PRIMARY KEY (trigger_id),
    CONSTRAINT trigger_definition_job_fk
        FOREIGN KEY (job_id)
        REFERENCES scheduler.job_definition (job_id),
    CONSTRAINT ck_trigger_definition_revision_positive CHECK (revision > 0),
    CONSTRAINT ck_trigger_definition_priority CHECK (priority BETWEEN 1 AND 10),
    CONSTRAINT ck_trigger_definition_spec_object CHECK (jsonb_typeof(spec) = 'object'),
    CONSTRAINT ck_trigger_definition_calendars_array CHECK (jsonb_typeof(calendar_names) = 'array'),
    CONSTRAINT ck_trigger_definition_state CHECK (state IN ('ACTIVE', 'PAUSED', 'DISABLED', 'DELETED')),
    CONSTRAINT ck_trigger_definition_window CHECK (end_at IS NULL OR start_at IS NULL OR end_at >= start_at),
    CONSTRAINT ck_trigger_definition_delete_metadata CHECK (
        (state = 'DELETED' AND deleted_at IS NOT NULL)
        OR (state <> 'DELETED' AND deleted_at IS NULL AND deleted_by IS NULL)
    )
);

CREATE UNIQUE INDEX ux_trigger_definition_namespace_name_active
    ON scheduler.trigger_definition (namespace, name)
    WHERE state <> 'DELETED';
CREATE INDEX ix_trigger_definition_job_state
    ON scheduler.trigger_definition (job_id, state);

CREATE TABLE scheduler.command_request (
    command_request_id uuid NOT NULL,
    request_id varchar(200) NOT NULL,
    command_type varchar(80) NOT NULL,
    namespace varchar(120) NOT NULL,
    aggregate_id uuid,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status varchar(32) NOT NULL DEFAULT 'RECEIVED',
    requested_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    last_error text,
    CONSTRAINT command_request_pkey PRIMARY KEY (command_request_id),
    CONSTRAINT uq_command_request_request_id UNIQUE (request_id),
    CONSTRAINT ck_command_request_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_command_request_status CHECK (status IN ('RECEIVED', 'PROCESSING', 'COMPLETED', 'FAILED'))
);

CREATE INDEX ix_command_request_status_requested_at
    ON scheduler.command_request (status, requested_at);

CREATE TABLE scheduler.inbox_message (
    inbox_message_id uuid NOT NULL,
    message_id varchar(200) NOT NULL,
    source varchar(120) NOT NULL,
    source_topic varchar(249),
    source_partition integer,
    source_offset bigint,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    received_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    last_error text,
    CONSTRAINT inbox_message_pkey PRIMARY KEY (inbox_message_id),
    CONSTRAINT uq_inbox_message_message_id UNIQUE (message_id),
    CONSTRAINT ck_inbox_message_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_inbox_message_headers_object CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT ck_inbox_message_source_position CHECK (
        (source_topic IS NULL AND source_partition IS NULL AND source_offset IS NULL)
        OR (source_topic IS NOT NULL AND source_partition IS NOT NULL AND source_offset IS NOT NULL)
    )
);

CREATE INDEX ix_inbox_message_unprocessed
    ON scheduler.inbox_message (received_at)
    WHERE processed_at IS NULL;

CREATE TABLE scheduler.execution (
    execution_id uuid NOT NULL,
    job_id uuid NOT NULL,
    trigger_id uuid,
    manual_fire_id uuid,
    scheduled_fire_time timestamptz,
    actual_fire_time timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    heartbeat_at timestamptz,
    status varchar(32) NOT NULL DEFAULT 'SCHEDULED',
    attempt integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    result jsonb,
    error jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT execution_pkey PRIMARY KEY (execution_id),
    CONSTRAINT execution_job_fk
        FOREIGN KEY (job_id)
        REFERENCES scheduler.job_definition (job_id),
    CONSTRAINT execution_trigger_fk
        FOREIGN KEY (trigger_id)
        REFERENCES scheduler.trigger_definition (trigger_id),
    CONSTRAINT ck_execution_attempt_positive CHECK (attempt > 0),
    CONSTRAINT ck_execution_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_execution_result_object CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
    CONSTRAINT ck_execution_error_object CHECK (error IS NULL OR jsonb_typeof(error) = 'object'),
    CONSTRAINT ck_execution_status CHECK (status IN ('SCHEDULED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'MISFIRED')),
    CONSTRAINT ck_execution_completed_after_started CHECK (
        completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at
    ),
    CONSTRAINT ck_execution_fire_identity CHECK (
        (trigger_id IS NOT NULL AND scheduled_fire_time IS NOT NULL AND manual_fire_id IS NULL)
        OR (manual_fire_id IS NOT NULL AND trigger_id IS NULL)
    )
);

CREATE UNIQUE INDEX ux_execution_scheduled_fire
    ON scheduler.execution (trigger_id, scheduled_fire_time)
    WHERE trigger_id IS NOT NULL AND scheduled_fire_time IS NOT NULL;
CREATE UNIQUE INDEX ux_execution_manual_fire
    ON scheduler.execution (manual_fire_id)
    WHERE manual_fire_id IS NOT NULL;
CREATE INDEX ix_execution_job_status
    ON scheduler.execution (job_id, status);
CREATE INDEX ix_execution_running_heartbeat
    ON scheduler.execution (heartbeat_at)
    WHERE status = 'RUNNING';

CREATE TABLE scheduler.outbox_event (
    outbox_event_id uuid NOT NULL,
    aggregate_type varchar(120) NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type varchar(250) NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    publish_attempts integer NOT NULL DEFAULT 0,
    last_error text,
    CONSTRAINT outbox_event_pkey PRIMARY KEY (outbox_event_id),
    CONSTRAINT ck_outbox_event_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_outbox_event_headers_object CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT ck_outbox_event_attempts_non_negative CHECK (publish_attempts >= 0)
);

CREATE INDEX ix_outbox_event_unpublished
    ON scheduler.outbox_event (occurred_at)
    WHERE published_at IS NULL;
CREATE INDEX ix_outbox_event_aggregate
    ON scheduler.outbox_event (aggregate_type, aggregate_id, occurred_at);

CREATE TABLE scheduler.audit_event (
    audit_event_id uuid NOT NULL,
    entity_type varchar(120) NOT NULL,
    entity_id uuid NOT NULL,
    action varchar(80) NOT NULL,
    actor varchar(200) NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    correlation_id varchar(200),
    CONSTRAINT audit_event_pkey PRIMARY KEY (audit_event_id),
    CONSTRAINT ck_audit_event_payload_object CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX ix_audit_event_entity
    ON scheduler.audit_event (entity_type, entity_id, occurred_at);
CREATE INDEX ix_audit_event_correlation
    ON scheduler.audit_event (correlation_id)
    WHERE correlation_id IS NOT NULL;
