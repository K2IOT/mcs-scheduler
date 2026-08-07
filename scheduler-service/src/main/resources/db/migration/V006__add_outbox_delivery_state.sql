ALTER TABLE scheduler.outbox_event
    ADD COLUMN state varchar(32) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN next_attempt_at timestamptz,
    ADD COLUMN claim_id uuid,
    ADD COLUMN claim_until timestamptz;

UPDATE scheduler.outbox_event
SET state = 'PUBLISHED'
WHERE published_at IS NOT NULL;

ALTER TABLE scheduler.outbox_event
    ADD CONSTRAINT ck_outbox_event_state
        CHECK (state IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'DEAD')),
    ADD CONSTRAINT ck_outbox_event_publish_state
        CHECK ((state = 'PUBLISHED') = (published_at IS NOT NULL)),
    ADD CONSTRAINT ck_outbox_event_claim_state
        CHECK (
            (state = 'IN_PROGRESS' AND claim_id IS NOT NULL AND claim_until IS NOT NULL)
            OR (state <> 'IN_PROGRESS' AND claim_id IS NULL AND claim_until IS NULL)
        );

CREATE INDEX ix_outbox_event_claimable
    ON scheduler.outbox_event (state, next_attempt_at, occurred_at)
    WHERE state IN ('PENDING', 'IN_PROGRESS');

ALTER TABLE scheduler.execution
    DROP CONSTRAINT ck_execution_status;

ALTER TABLE scheduler.execution
    ADD CONSTRAINT ck_execution_status
    CHECK (status IN (
        'SCHEDULED',
        'SUPPRESSED',
        'RUNNING',
        'SUCCEEDED',
        'FAILED',
        'CANCELLED',
        'MISFIRED',
        'DELIVERED',
        'DELIVERY_FAILED'
    ));
