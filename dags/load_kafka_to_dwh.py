from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.sdk import dag, task, task_group
from datetime import datetime
from airflow.providers.oracle.hooks.oracle import OracleHook

@dag(
    start_date=datetime(2026, 3, 10),
    schedule=None,
    catchup=False,
)
def load_kafka_to_oracle():

    @task
    def read_from_kafka():
        return None
    
    @task
    def load_to_oracle():
        return None
    
    read_from_kafka >> load_to_oracle

load_kafka_to_oracle()