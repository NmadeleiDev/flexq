from datetime import datetime
from typing import List, Tuple, Union
from flexq.job import Job, JobStatusEnum
from flexq.jobstores.jobstore_base import JobStoreBase
import psycopg2

schema_name = 'flexq'

job_instances_table_name = 'flexq_job'

interval_name_enum_name = 'flexq_interval_name'

from .tables_create_sql import job_instances_table_create_query, job_status_enum_create_query, schema_create_query


class PostgresJobStore(JobStoreBase):
    def __init__(self, dsn: str, instance_name='default') -> None:
        super().__init__(instance_name)
        self.dsn = dsn

        self.job_instances_table_name = job_instances_table_name + '__' + self.instance_name

    def init_conn(self):
        self._init_db()
        self._init_tables()

    def _init_db(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def _init_tables(self):
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(schema_create_query(schema_name))
                curs.execute(job_status_enum_create_query)
                curs.execute(job_instances_table_create_query(schema_name, self.job_instances_table_name))

    def try_acknowledge_job(self, job_id: str, worker_heartbeat_interval_seconds: int) -> bool:
        query = f"""
        UPDATE {schema_name}.{self.job_instances_table_name} SET status = %s, worker_heartbeat_interval_seconds = %s 
        WHERE id = %s AND status = %s
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (JobStatusEnum.acknowledged.value, worker_heartbeat_interval_seconds, job_id,
                                     JobStatusEnum.created.value))
                return curs.rowcount == 1

    def set_status_for_job(self, job_id: str, status: JobStatusEnum) -> None:
        if status in (JobStatusEnum.success.value, JobStatusEnum.failed.value):
            query = f"""
            UPDATE {schema_name}.{self.job_instances_table_name} SET status = %s, finished_at = now() WHERE id = %s 
            """
        else:
            query = f"""
            UPDATE {schema_name}.{self.job_instances_table_name} SET status = %s WHERE id = %s 
            """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (status, job_id))

    def save_result_for_job(self, job_id: str, result: bytes) -> None:
        query = f"""
        UPDATE {schema_name}.{self.job_instances_table_name} SET result = %s WHERE id = %s 
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (result, job_id))

    def add_job_to_store(self, job: Job) -> str:
        insert_fields = 'job_queue_name, args, kwargs, parent_job_id, retry_until_success, retry_delay_minutes, name, cron, interval_name, interval_value'

        query_args = (
        job.queue_name, job.get_args_bytes(), job.get_kwargs_bytes(), job.parent_job_id, job.retry_until_success,
        job.retry_delay_minutes, job.name, job.cron, job.interval_name, job.interval_value)

        with psycopg2.connect(self.dsn) as conn:

            if job.id is None:
                query = f"""
                INSERT INTO {schema_name}.{self.job_instances_table_name} ({insert_fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING ID
                """
                with conn.cursor() as curs:
                    curs.execute(query, query_args)
                    job.id = str(curs.fetchone()[0])
            else:
                query = f"""
                INSERT INTO {schema_name}.{self.job_instances_table_name} (id, {insert_fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                with conn.cursor() as curs:
                    curs.execute(query, [job.id, ] + query_args)

        return job.id

    def remove_job_from_store(self, job_id: str):
        query = f"""
        DELETE FROM {schema_name}.{self.job_instances_table_name} WHERE id = %s
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (job_id,))

    def get_child_job_ids(self, parent_job_id: str) -> List[str]:
        query = f"""
        SELECT id FROM {schema_name}.{self.job_instances_table_name} WHERE parent_job_id = %s ORDER BY id
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (parent_job_id,))
                return [x[0] for x in curs.fetchall()]

    def get_jobs(self, job_id: Union[str, None] = None, include_result=False, with_schedule_only=False,
                 retry_until_success_only=False,
                 heartbeat_missed_by_more_than_n_seconds: Union[int, None] = None,
                 status: Union[JobStatusEnum, None] = None) -> Union[List[Job], None]:

        fields_to_select = "job_queue_name, args, kwargs, status, parent_job_id, retry_until_success, " \
                           "retry_delay_minutes, name, cron, interval_name, interval_value, id, created_at, " \
                           "finished_at, last_heartbeat_ts, start_timestamp "

        if include_result:
            fields_to_select += ", result"

        where_part = []
        args = []
        if job_id is not None:
            where_part.append('id = %s')
            args.append(job_id)
        if with_schedule_only:
            where_part.append('cron is not null or interval_name is not null')
        if retry_until_success_only:
            where_part.append('retry_until_success = true')
        if heartbeat_missed_by_more_than_n_seconds is not None:
            where_part.append(
                "(last_heartbeat_ts is null) or (EXTRACT(EPOCH FROM (now() - last_heartbeat_ts)) - worker_heartbeat_interval_seconds) > %s")
            args.append(heartbeat_missed_by_more_than_n_seconds)
        if status is not None:
            where_part.append('status = %s')
            args.append(status)

        if len(where_part) > 0:
            where_part_str = ' WHERE ' + ' AND '.join([f'({x})' for x in where_part])
        else:
            where_part_str = ''

        query = f"""
        SELECT {fields_to_select} FROM {schema_name}.{self.job_instances_table_name} {where_part_str} 
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, args)
                results = curs.fetchall()

                if results is None or len(results) == 0:
                    return None

                jobs = []
                for result in results:
                    job = Job(
                        queue_name=result[0],
                        status=result[3],
                        parent_job_id=result[4],
                        retry_until_success=result[5],
                        retry_delay_minutes=result[6],
                        name=result[7],
                        cron=result[8],
                        interval_name=result[9],
                        interval_value=result[10],
                        id=result[11],
                        created_at=result[12],
                        finished_at=result[13],
                        last_heartbeat_ts=result[14],
                        start_timestamp=result[15],
                    )
                    job.set_args_bytes(result[1])
                    job.set_kwargs_bytes(result[2])
                    if include_result:
                        job.set_result_bytes(result[16])

                    jobs.append(job)

                return jobs

    def get_not_acknowledged_jobs_ids_and_queue_names(self) -> List[Tuple[str, str]]:
        query = f"""
        SELECT rj.id, rj.job_queue_name FROM {schema_name}.{self.job_instances_table_name} as rj
        LEFT JOIN {schema_name}.{self.job_instances_table_name} as pj ON rj.parent_job_id = pj.id
        WHERE 
            rj.status = '{JobStatusEnum.created.value}' 
            AND (rj.parent_job_id IS NULL OR pj.status = '{JobStatusEnum.ephemeral.value}')
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query)
                result = curs.fetchall()

                return [(x[0], x[1]) for x in result]

    def get_job_user_status(self, job_id: str) -> str:
        query = f"""
        SELECT user_status FROM {schema_name}.{self.job_instances_table_name} WHERE id = %s
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (job_id,))
                return curs.fetchone()[0]

    def set_job_user_status(self, job_id: str, value: str):
        query = f"""
        UPDATE {schema_name}.{self.job_instances_table_name} SET user_status = %s WHERE id = %s
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (value, job_id))

    def set_job_parent_id(self, job_id: str, parent_job_id: str):
        query = f"""
        UPDATE {schema_name}.{self.job_instances_table_name} SET parent_job_id = %s WHERE id = %s
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (parent_job_id, job_id))

    def set_job_last_heartbeat_ts_to_now(self, job_id: str, set_start_ts_also=False):
        if set_start_ts_also:
            query = f"""
            UPDATE {schema_name}.{self.job_instances_table_name} SET last_heartbeat_ts = now(), start_timestamp = now() WHERE id = %s
            """
        else:
            query = f"""
            UPDATE {schema_name}.{self.job_instances_table_name} SET last_heartbeat_ts = now() WHERE id = %s
            """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (job_id,))

    def set_job_start_ts(self, job_id: str, start_timestamp: datetime):
        query = f"""
        UPDATE {schema_name}.{self.job_instances_table_name} SET start_timestamp = %s WHERE id = %s
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(query, (start_timestamp, job_id))
