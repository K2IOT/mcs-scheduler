CREATE SCHEMA IF NOT EXISTS quartz;

COMMENT ON SCHEMA quartz IS 'Quartz 2.5.x JDBC JobStore tables';

CREATE TABLE quartz.qrtz_job_details (
    sched_name varchar(120) NOT NULL,
    job_name varchar(200) NOT NULL,
    job_group varchar(200) NOT NULL,
    description varchar(250),
    job_class_name varchar(250) NOT NULL,
    is_durable boolean NOT NULL,
    is_nonconcurrent boolean NOT NULL,
    is_update_data boolean NOT NULL,
    requests_recovery boolean NOT NULL,
    job_data bytea,
    CONSTRAINT qrtz_job_details_pkey PRIMARY KEY (sched_name, job_name, job_group)
);

CREATE TABLE quartz.qrtz_triggers (
    sched_name varchar(120) NOT NULL,
    trigger_name varchar(200) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    job_name varchar(200) NOT NULL,
    job_group varchar(200) NOT NULL,
    description varchar(250),
    next_fire_time bigint,
    prev_fire_time bigint,
    priority integer,
    trigger_state varchar(16) NOT NULL,
    trigger_type varchar(8) NOT NULL,
    start_time bigint NOT NULL,
    end_time bigint,
    calendar_name varchar(200),
    misfire_instr smallint,
    job_data bytea,
    CONSTRAINT qrtz_triggers_pkey PRIMARY KEY (sched_name, trigger_name, trigger_group),
    CONSTRAINT qrtz_triggers_job_details_fk
        FOREIGN KEY (sched_name, job_name, job_group)
        REFERENCES quartz.qrtz_job_details (sched_name, job_name, job_group)
);

CREATE TABLE quartz.qrtz_simple_triggers (
    sched_name varchar(120) NOT NULL,
    trigger_name varchar(200) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    repeat_count bigint NOT NULL,
    repeat_interval bigint NOT NULL,
    times_triggered bigint NOT NULL,
    CONSTRAINT qrtz_simple_triggers_pkey PRIMARY KEY (sched_name, trigger_name, trigger_group),
    CONSTRAINT qrtz_simple_triggers_trigger_fk
        FOREIGN KEY (sched_name, trigger_name, trigger_group)
        REFERENCES quartz.qrtz_triggers (sched_name, trigger_name, trigger_group)
        ON DELETE CASCADE
);

CREATE TABLE quartz.qrtz_cron_triggers (
    sched_name varchar(120) NOT NULL,
    trigger_name varchar(200) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    cron_expression varchar(120) NOT NULL,
    time_zone_id varchar(80),
    CONSTRAINT qrtz_cron_triggers_pkey PRIMARY KEY (sched_name, trigger_name, trigger_group),
    CONSTRAINT qrtz_cron_triggers_trigger_fk
        FOREIGN KEY (sched_name, trigger_name, trigger_group)
        REFERENCES quartz.qrtz_triggers (sched_name, trigger_name, trigger_group)
        ON DELETE CASCADE
);

CREATE TABLE quartz.qrtz_simprop_triggers (
    sched_name varchar(120) NOT NULL,
    trigger_name varchar(200) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    str_prop_1 varchar(512),
    str_prop_2 varchar(512),
    str_prop_3 varchar(512),
    int_prop_1 integer,
    int_prop_2 integer,
    long_prop_1 bigint,
    long_prop_2 bigint,
    dec_prop_1 numeric(13,4),
    dec_prop_2 numeric(13,4),
    bool_prop_1 boolean,
    bool_prop_2 boolean,
    CONSTRAINT qrtz_simprop_triggers_pkey PRIMARY KEY (sched_name, trigger_name, trigger_group),
    CONSTRAINT qrtz_simprop_triggers_trigger_fk
        FOREIGN KEY (sched_name, trigger_name, trigger_group)
        REFERENCES quartz.qrtz_triggers (sched_name, trigger_name, trigger_group)
        ON DELETE CASCADE
);

CREATE TABLE quartz.qrtz_blob_triggers (
    sched_name varchar(120) NOT NULL,
    trigger_name varchar(200) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    blob_data bytea,
    CONSTRAINT qrtz_blob_triggers_pkey PRIMARY KEY (sched_name, trigger_name, trigger_group),
    CONSTRAINT qrtz_blob_triggers_trigger_fk
        FOREIGN KEY (sched_name, trigger_name, trigger_group)
        REFERENCES quartz.qrtz_triggers (sched_name, trigger_name, trigger_group)
        ON DELETE CASCADE
);

CREATE TABLE quartz.qrtz_calendars (
    sched_name varchar(120) NOT NULL,
    calendar_name varchar(200) NOT NULL,
    calendar bytea NOT NULL,
    CONSTRAINT qrtz_calendars_pkey PRIMARY KEY (sched_name, calendar_name)
);

CREATE TABLE quartz.qrtz_paused_trigger_grps (
    sched_name varchar(120) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    CONSTRAINT qrtz_paused_trigger_grps_pkey PRIMARY KEY (sched_name, trigger_group)
);

CREATE TABLE quartz.qrtz_fired_triggers (
    sched_name varchar(120) NOT NULL,
    entry_id varchar(95) NOT NULL,
    trigger_name varchar(200) NOT NULL,
    trigger_group varchar(200) NOT NULL,
    instance_name varchar(200) NOT NULL,
    fired_time bigint NOT NULL,
    sched_time bigint NOT NULL,
    priority integer NOT NULL,
    state varchar(16) NOT NULL,
    job_name varchar(200),
    job_group varchar(200),
    is_nonconcurrent boolean,
    requests_recovery boolean,
    CONSTRAINT qrtz_fired_triggers_pkey PRIMARY KEY (sched_name, entry_id)
);

CREATE TABLE quartz.qrtz_scheduler_state (
    sched_name varchar(120) NOT NULL,
    instance_name varchar(200) NOT NULL,
    last_checkin_time bigint NOT NULL,
    checkin_interval bigint NOT NULL,
    CONSTRAINT qrtz_scheduler_state_pkey PRIMARY KEY (sched_name, instance_name)
);

CREATE TABLE quartz.qrtz_locks (
    sched_name varchar(120) NOT NULL,
    lock_name varchar(40) NOT NULL,
    CONSTRAINT qrtz_locks_pkey PRIMARY KEY (sched_name, lock_name)
);

CREATE INDEX idx_qrtz_j_req_recovery
    ON quartz.qrtz_job_details (sched_name, requests_recovery);
CREATE INDEX idx_qrtz_j_grp
    ON quartz.qrtz_job_details (sched_name, job_group);

CREATE INDEX idx_qrtz_t_j
    ON quartz.qrtz_triggers (sched_name, job_name, job_group);
CREATE INDEX idx_qrtz_t_jg
    ON quartz.qrtz_triggers (sched_name, job_group);
CREATE INDEX idx_qrtz_t_c
    ON quartz.qrtz_triggers (sched_name, calendar_name);
CREATE INDEX idx_qrtz_t_g
    ON quartz.qrtz_triggers (sched_name, trigger_group);
CREATE INDEX idx_qrtz_t_state
    ON quartz.qrtz_triggers (sched_name, trigger_state);
CREATE INDEX idx_qrtz_t_n_state
    ON quartz.qrtz_triggers (sched_name, trigger_name, trigger_group, trigger_state);
CREATE INDEX idx_qrtz_t_n_g_state
    ON quartz.qrtz_triggers (sched_name, trigger_group, trigger_state);
CREATE INDEX idx_qrtz_t_next_fire_time
    ON quartz.qrtz_triggers (sched_name, next_fire_time);
CREATE INDEX idx_qrtz_t_nft_st
    ON quartz.qrtz_triggers (sched_name, trigger_state, next_fire_time);
CREATE INDEX idx_qrtz_t_nft_misfire
    ON quartz.qrtz_triggers (sched_name, misfire_instr, next_fire_time);
CREATE INDEX idx_qrtz_t_nft_st_misfire
    ON quartz.qrtz_triggers (sched_name, misfire_instr, next_fire_time, trigger_state);
CREATE INDEX idx_qrtz_t_nft_st_misfire_grp
    ON quartz.qrtz_triggers (sched_name, misfire_instr, next_fire_time, trigger_group, trigger_state);

CREATE INDEX idx_qrtz_ft_trig_inst_name
    ON quartz.qrtz_fired_triggers (sched_name, instance_name);
CREATE INDEX idx_qrtz_ft_inst_job_req_rcvry
    ON quartz.qrtz_fired_triggers (sched_name, instance_name, requests_recovery);
CREATE INDEX idx_qrtz_ft_j_g
    ON quartz.qrtz_fired_triggers (sched_name, job_name, job_group);
CREATE INDEX idx_qrtz_ft_jg
    ON quartz.qrtz_fired_triggers (sched_name, job_group);
CREATE INDEX idx_qrtz_ft_t_g
    ON quartz.qrtz_fired_triggers (sched_name, trigger_name, trigger_group);
CREATE INDEX idx_qrtz_ft_tg
    ON quartz.qrtz_fired_triggers (sched_name, trigger_group);
