from airflow.sdk import dag, task, task_group
from datetime import datetime

@dag(
    start_date=datetime(2026, 3, 10),
    schedule=None,
    catchup=False,
)
def load_kafka_to_oracle():

    from module.database_functions import load_db_to_db

    LOAD_ORACLE_TO_ORACLE = load_db_to_db.override(task_id="LOAD_ORACLE_TO_ORACLE")(
        source_conn_id="ORACLE_TEST_DWH",
        source_table="EMPLOYEES",
        source_schema="TEST_SOURCE",
        target_conn_id="ORACLE_TEST_DWH",
        target_schema="TEST_TARGET",
        target_table="EMPLOYEES",
        chunksize=5000,
        if_truncate=True
    )
    
load_kafka_to_oracle()