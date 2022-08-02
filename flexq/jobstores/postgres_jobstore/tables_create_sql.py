from flexq.job import Job, JobStatusEnum

schema_name = 'flexq'

queues_table_name = 'flexq_queue'
job_instances_table_name = 'flexq_job'
job_status_enum_name = 'flexq_job_status'
execution_pool_table_name = 'flexq_execution_pool'

schema_create_query = f"""CREATE SCHEMA IF NOT EXISTS {schema_name}"""

job_instances_table_create_query = f"""
create table if not exists {schema_name}.{job_instances_table_name}
(
    id           serial
        constraint table_name_pk
            primary key,
    job_queue_name    varchar   not null,
    args            bytea not null,
    kwargs          bytea not null,
    start_after_job_instance_id     int default null,

    created_at             timestamp default now()
)
"""

job_status_enum_create_query = f"""
DO $$ BEGIN
    CREATE TYPE {job_status_enum_name} AS ENUM ({', '.join(map(lambda x: "'" + x + "'", JobStatusEnum))});
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
"""

execution_pool_table_create_query = f"""
create table if not exists {schema_name}.{execution_pool_table_name}
(
    id           serial
        constraint table_name_pk
            primary key,
    job_instance_id    int unique   not null,
    status             {job_status_enum_name} default {JobStatusEnum.acknowledged},
    result             bytea default null,

    created_at             timestamp default now(),

    CONSTRAINT fk_{job_instances_table_name}
      FOREIGN KEY(job_instance_id) 
	  REFERENCES {job_instances_table_name}(id)
      ON DELETE CASCADE
)
"""