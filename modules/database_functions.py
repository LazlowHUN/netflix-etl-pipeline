import pandas as pd
from sqlalchemy import text
import sys
import oracledb

# from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
# from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

def load_db_to_db(source_conn_id, source_table, source_schema, target_conn_id, target_schema, target_table, chunksize, if_truncate=True):
    from airflow.providers.oracle.hooks.oracle import OracleHook

    src_hook = OracleHook(oracle_conn_id=source_conn_id)
    source_engine = src_hook.get_sqlalchemy_engine()

    if source_conn_id == target_conn_id:
        target_engine = source_engine
    else:
        tgt_hook = OracleHook(oracle_conn_id=target_conn_id)
        target_engine = tgt_hook.get_sqlalchemy_engine()
    
    with target_engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {target_schema}.{target_table}"))
        conn.commit()

    with source_engine.connect().execution_options(stream_results=True) as src_conn:
        chunk = pd.read_sql(f"SELECT * FROM {source_schema}.{source_table}", source_engine, index_col=None, chunksize=chunksize)
        
        for i, chunk_df in enumerate(chunk):
            print(f"Processing chunk {i+1}...")
            
            chunk_df.to_sql(
                name=target_table,
                con=target_engine,
                schema=target_schema,
                if_exists='append',
                index=False,
                chunksize=chunksize
            )
            
    print(f"Data moved from {source_table} to {target_table}")