from flexq.job import Job, JobIntervalNameEnum, JobStatusEnum

job_status_enum_name = 'flexq_job_status'

schema_create_query = lambda schema_name: f"""CREATE SCHEMA IF NOT EXISTS {schema_name}"""

job_instances_table_create_query = lambda schema_name, job_instances_table_name : f"""
create table if not exists {schema_name}.{job_instances_table_name}
(
    id           serial
        constraint {schema_name}_{job_instances_table_name}_pk
            primary key,
    job_queue_name    varchar   not null,
    args            bytea not null,
    kwargs          bytea not null,
    parent_job_id     int default null,
    start_when_other_job_id_success int default null,
    result             bytea default null,

    cron             varchar default null,
    interval_name             varchar default null,
    interval_value             integer default 0,
    
    retry_until_success             boolean default false,
    retry_delay_minutes               integer default 0,

    name varchar default null,

    status             {job_status_enum_name} default '{JobStatusEnum.created}',

    user_status varchar default null,

    last_heartbeat_ts   timestamp default null,
    worker_heartbeat_interval_seconds   integer default 0,

    created_at             timestamp default now(),
    finished_at             timestamp default null,
    start_timestamp             timestamp default null,

    CONSTRAINT fk_{job_instances_table_name}
      FOREIGN KEY(parent_job_id) 
	  REFERENCES {schema_name}.{job_instances_table_name}(id)
      ON DELETE CASCADE
)
"""


job_status_enum_create_query = f"""
DO $$ BEGIN
    CREATE TYPE {job_status_enum_name} AS ENUM ({', '.join(map(lambda x: "'" + x + "'", JobStatusEnum))});
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
"""
