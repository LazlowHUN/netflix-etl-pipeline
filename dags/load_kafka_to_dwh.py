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
        source_engine="",
        source_table="EMPLOYEES",
        target_engine="",
        target_schema="DWH",
        target_table="DIM_EMP",
        chunksize=5000
    )
    
load_kafka_to_oracle()