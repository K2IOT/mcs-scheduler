ALTER TABLE scheduler.trigger_definition
    ADD COLUMN pause_reason varchar(32);

UPDATE scheduler.trigger_definition
SET pause_reason = 'INDIVIDUAL'
WHERE state = 'PAUSED';

ALTER TABLE scheduler.trigger_definition
    ADD CONSTRAINT ck_trigger_definition_pause_reason
        CHECK (
            pause_reason IS NULL
            OR (state = 'PAUSED' AND pause_reason IN ('INDIVIDUAL', 'JOB'))
        );

CREATE INDEX ix_trigger_definition_job_pause_reason
    ON scheduler.trigger_definition (job_id, state, pause_reason)
    WHERE state = 'PAUSED';
