ALTER TABLE scheduler.job_definition
    ADD COLUMN durable boolean NOT NULL DEFAULT true;

ALTER TABLE scheduler.command_request
    ADD COLUMN request_hash varchar(64),
    ADD COLUMN response_json jsonb;

UPDATE scheduler.command_request
SET request_hash = repeat('0', 64)
WHERE request_hash IS NULL;

ALTER TABLE scheduler.command_request
    ALTER COLUMN request_hash SET NOT NULL;

ALTER TABLE scheduler.command_request
    ADD CONSTRAINT ck_command_request_hash_sha256
        CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT ck_command_request_response_json
        CHECK (response_json IS NULL OR jsonb_typeof(response_json) = 'object');
