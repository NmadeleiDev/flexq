import logging
from typing import List
from flexq.job import Job, JobStatusEnum
from flexq.jobstores.jobstore_base import JobStoreBase
import psycopg2

from .tables_create_sql import job_instances_table_create_query, job_status_enum_create_query, execution_pool_table_create_query, schema_name, job_instances_table_name, execution_pool_table_name

class PostgresJobStore(JobStoreBase):
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _init_db(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def _init_tables(self):
        with self.conn.cursor() as curs:
            curs.execute(job_instances_table_create_query)
            curs.execute(job_status_enum_create_query)
            curs.execute(execution_pool_table_create_query)

    def try_acknowledge_job(self, job_id: str) -> bool:
        query = f"""
        INSERT INTO {schema_name}.{execution_pool_table_name} (job_instance_id, ) VALUES (%s,) 
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id,))

    def set_status_for_job(self, job_id: str, status: JobStatusEnum) -> None:
        query = f"""
        UPDATE {schema_name}.{execution_pool_table_name} SET status = %s WHERE job_instance_id = %s 
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (status, job_id))

    def add_job_to_queue(self, job: Job) -> str:
        if job.id is None:
            query = f"""
            INSERT INTO {schema_name}.{job_instances_table_name} (job_queue_name, args, kwargs) VALUES (%s, %s, %s) RETURNING ID
            """
            with self.conn.cursor() as curs:
                curs.execute(query, (job.queue_name, job.get_args_bytes(), job.get_kwargs_bytes()))
                job.id = curs.fetchone()[0]
        else:
            query = f"""
            INSERT INTO {schema_name}.{job_instances_table_name} (id, job_queue_name, args, kwargs) VALUES (%s, %s, %s, %s)
            """
            with self.conn.cursor() as curs:
                curs.execute(query, (job.id, job.queue_name, job.get_args_bytes(), job.get_kwargs_bytes()))
        return job.id

    def get_job(self, job_id: str) -> Job:
        query = f"""
        SELECT (job_queue_name, args, kwargs) FROM {schema_name}.{job_instances_table_name} WHERE id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id, ))
            result = curs.fetchone()

            job = Job(id=job_id, queue_name=result[0])
            job.set_args_bytes(result[1])
            job.set_kwargs_bytes(result[2])

    def get_jobs_ids_in_queues_waiting_for_job(self, job_id: str, queues_names: str) -> List[str]:
        query = f"""
        SELECT id FROM {schema_name}.{job_instances_table_name} WHERE start_after_job_instance_id = %s AND job_queue_name IN %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id, queues_names))
            result = curs.fetchmany()

            return [x[0] for x in result]

    def get_queue_names_interested_in_job(self, job_id: str) -> List[str]:
        query = f"""
        SELECT DISTINCT job_queue_name FROM {schema_name}.{job_instances_table_name} WHERE start_after_job_instance_id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id, ))
            result = curs.fetchmany()

            return [x[0] for x in result]

