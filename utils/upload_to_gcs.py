import os
import pandas as pd

from google.cloud import storage

project_id = os.environ.get("project_id")
bucket_name = os.environ.get("bucket_name")
pasta_bucket = os.environ.get("pasta_bucket")

def upload_to_gcs(filename: str, data: pd.DataFrame):
    
    filename = filename
    
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{pasta_bucket}/{filename}')
    blob.upload_from_string(data.to_csv(index=False), content_type='text/csv')
    
    print(f"Arquivo '{filename}' enviado para o bucket '{bucket_name}' no diretório {pasta_bucket}.")