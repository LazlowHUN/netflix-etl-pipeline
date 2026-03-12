from airflow.sdk import dag, task, task_group
from datetime import datetime

@dag(
    start_date=datetime(2026, 3, 10),
    schedule=None,
    catchup=False,
)
def load_kafka_to_oracle():

    @task
    def load_from_db_to_db(source_conn_id, source_schema, source_table, target_conn_id, target_schema, target_table, chunksize=10000, if_truncate=True):
        from modules.database_functions import load_db_to_db
        load_db_to_db(source_conn_id=source_conn_id, source_schema=source_schema, source_table=source_table, target_conn_id=target_conn_id, target_schema=target_schema, target_table=target_table, chunksize=chunksize, if_truncate=if_truncate)

    LOAD_ORACLE_TO_ORACLE = load_from_db_to_db.override(task_id="LOAD_ORACLE_TO_ORACLE")(
        source_conn_id="ORACLE_TEST_DWH",
        source_schema="TEST_SOURCE",
        source_table="EMPLOYEES",
        target_conn_id="ORACLE_TEST_DWH",
        target_schema="TEST_TARGET",
        target_table="EMPLOYEES",
        chunksize=5000,
        if_truncate=True
    )
    
load_kafka_to_oracle()