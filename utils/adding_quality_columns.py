import os
import pytz
import pandas as pd

from datetime import datetime

bucket_name = os.environ.get("bucket_name")
pasta_bucket = os.environ.get("pasta_bucket")

def adding_quality_columns(filename):
    
    caminho = (f'gs://{bucket_name}/{pasta_bucket}/{filename}')
    
    df = pd.read_csv(f'gs://{bucket_name}/{pasta_bucket}/{filename}', delimiter=",")
    df = df.astype('str')
    
    timezone = pytz.timezone('America/Sao_Paulo')
    date_time = datetime.now(tz=timezone)
    date = date_time.strftime('%Y-%m-%d %H:%M:%S')
    
    df.insert(0, 'source', caminho)
    df.insert(0, 'uploaded_at', date)
    
    columns_to_remove = ['_id', 'usuario']
    
    for column in columns_to_remove:
        if column in df.columns:
            df.drop(columns=[column], inplace=True)
    
    return df 