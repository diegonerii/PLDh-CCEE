import os
import pandas as pd

from google.cloud import bigquery

project_id = os.environ.get("project_id")
dataset = os.environ.get("dataset")
table_name = os.environ.get("table_name")


def create_table_bigquery(df: pd.DataFrame):
    
    client_bq = bigquery.Client(project=project_id)
    table_id = f'{project_id}.{dataset}.{table_name}' 
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(name, 'STRING') for name in df.columns
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    try:
        job = client_bq.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"Dados carregados na tabela {table_id}")
    except Exception as e:
        print(f"Erro ao carregar dados na tabela {table_id}: {e}")
        if 'Not found: Table' in str(e):
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job = client_bq.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print(f"Tabela {table_id} criada e dados inseridos")
        else:
            raise e  