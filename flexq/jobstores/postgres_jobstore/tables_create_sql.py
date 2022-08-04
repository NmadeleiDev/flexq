from flexq.job import Job, JobStatusEnum

schema_name = 'flexq'

queues_table_name = 'flexq_queue'
job_instances_table_name = 'flexq_job'
job_status_enum_name = 'flexq_job_status'
job_results_table = 'flexq_job_result'

schema_create_query = f"""CREATE SCHEMA IF NOT EXISTS {schema_name}"""

job_instances_table_create_query = f"""
create table if not exists {schema_name}.{job_instances_table_name}
(
    id           serial
        constraint {schema_name}_{job_instances_table_name}_pk
            primary key,
    job_queue_name    varchar   not null,
    args            bytea not null,
    kwargs          bytea not null,
    start_after_job_instance_id     int default null,
    result             bytea default null,

    status             {job_status_enum_name} default '{JobStatusEnum.created}',

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

job_results_table_create_query = f"""
create table if not exists {schema_name}.{job_results_table}
(
    job_id           integer
        constraint {schema_name}_{job_results_table}_pk
            primary key,
    result             bytea default null,
    

    CONSTRAINT fk_{job_results_table}
      FOREIGN KEY(job_id) 
	  REFERENCES {schema_name}.{job_instances_table_name}(id)
      ON DELETE CASCADE
)
"""