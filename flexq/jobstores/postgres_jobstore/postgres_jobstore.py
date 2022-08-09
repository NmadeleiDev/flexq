from typing import List, Tuple
from flexq.exceptions.jobstore import JobNotFoundInStore
from flexq.job import Job, JobStatusEnum
from flexq.jobstores.jobstore_base import JobStoreBase
import psycopg2
from psycopg2.errors import UniqueViolation

from .tables_create_sql import job_instances_table_create_query, job_status_enum_create_query, schema_name, schema_create_query, job_instances_table_name 

class PostgresJobStore(JobStoreBase):
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._init_db()
        self._init_tables()

    def _init_db(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def _init_tables(self):
        with self.conn.cursor() as curs:
            curs.execute(schema_create_query)
            curs.execute(job_status_enum_create_query)
            curs.execute(job_instances_table_create_query)

    def try_acknowledge_job(self, job_id: str) -> bool:
        query = f"""
        UPDATE {schema_name}.{job_instances_table_name} SET status = %s 
        WHERE id = %s AND status = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (JobStatusEnum.acknowledged.value, job_id, JobStatusEnum.created.value))
            return curs.rowcount == 1 # TODO: протестить, что rowcount == 1 всегда только в одном вызове

    def set_status_for_job(self, job_id: str, status: JobStatusEnum) -> None:
        if status in (JobStatusEnum.success.value, JobStatusEnum.failed.value):
            query = f"""
            UPDATE {schema_name}.{job_instances_table_name} SET status = %s, finished_at = now() WHERE id = %s 
            """
        else:
            query = f"""
            UPDATE {schema_name}.{job_instances_table_name} SET status = %s WHERE id = %s 
            """
        with self.conn.cursor() as curs:
            curs.execute(query, (status, job_id))

    def save_result_for_job(self, job_id: str, result: bytes) -> None:
        query = f"""
        UPDATE {schema_name}.{job_instances_table_name} SET result = %s WHERE id = %s 
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (result, job_id))

    def add_job_to_store(self, job: Job) -> str:
        insert_fields = 'job_queue_name, args, kwargs, parent_job_id, retry_until_success, retry_delay_minutes, name'
        
        query_args = (job.queue_name, job.get_args_bytes(), job.get_kwargs_bytes(), job.parent_job_id, job.retry_until_success, job.retry_delay_minutes, job.name)

        if job.id is None:
            query = f"""
            INSERT INTO {schema_name}.{job_instances_table_name} ({insert_fields}) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING ID
            """
            with self.conn.cursor() as curs:
                curs.execute(query, query_args)
                job.id = str(curs.fetchone()[0])
        else:
            query = f"""
            INSERT INTO {schema_name}.{job_instances_table_name} (id, {insert_fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            with self.conn.cursor() as curs:
                curs.execute(query, (job.id, ) + query_args)

        return job.id

    def update_job_in_store(self, job: Job) -> str:
        query = f"""
        UPDATE {schema_name}.{job_instances_table_name} SET (job_queue_name, args, kwargs, parent_job_id) = (%s, %s, %s, %s) WHERE id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job.queue_name, job.get_args_bytes(), job.get_kwargs_bytes(), job.parent_job_id, job.id))

    def remove_job_from_store(self, job_id: str):
        query = f"""
        DELETE FROM {schema_name}.{job_instances_table_name} WHERE id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id, ))

    def get_child_job_ids(self, parent_job_id: str) -> List[str]:
        query = f"""
        SELECT id FROM {schema_name}.{job_instances_table_name} WHERE parent_job_id = %s ORDER BY id
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (parent_job_id,))
            return [x[0] for x in curs.fetchall()]

    def get_job(self, job_id: str, include_result=False) -> Job:
        fields_to_select = "job_queue_name, args, kwargs, status, parent_job_id, retry_until_success, retry_delay_minutes, name"

        if include_result:
            fields_to_select += ", result"

        query = f"""
        SELECT {fields_to_select} FROM {schema_name}.{job_instances_table_name} WHERE id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id, ))
            result = curs.fetchone()

            if result is None:
                raise JobNotFoundInStore(f'Job id = {job_id} not found in store')

            job = Job(
                id=job_id, 
                queue_name=result[0], 
                status=result[3], 
                parent_job_id=result[4],
                retry_until_success=result[5],
                retry_delay_minutes=result[6],
                name=result[7]
                )
            job.set_args_bytes(result[1])
            job.set_kwargs_bytes(result[2])
            if include_result:
                job.set_result_bytes(result[8])

            return job

    def get_not_acknowledged_jobs_ids_and_queue_names(self) -> List[Tuple[str, str]]:
        query = f"""
        SELECT id, job_queue_name FROM {schema_name}.{job_instances_table_name}
        WHERE 
            status = 'created' 
            AND parent_job_id IS NULL 
        """
        with self.conn.cursor() as curs:
            curs.execute(query)
            result = curs.fetchall()

            return [(x[0], x[1]) for x in result]

    def get_job_user_status(self, job_id: str) -> str:
        query = f"""
        SELECT user_status FROM {schema_name}.{job_instances_table_name} WHERE id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (job_id,))
            return curs.fetchone[0]

    def set_job_user_status(self, job_id: str, value: str):
        query = f"""
        UPDATE {schema_name}.{job_instances_table_name} SET user_status = %s WHERE id = %s
        """
        with self.conn.cursor() as curs:
            curs.execute(query, (value, job_id))
