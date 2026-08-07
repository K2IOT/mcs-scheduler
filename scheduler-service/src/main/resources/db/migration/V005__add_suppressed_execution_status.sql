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
        'MISFIRED'
    ));
