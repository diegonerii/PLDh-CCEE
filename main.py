import os
import requests
import pytz
import pandas as pd

from bs4 import BeautifulSoup
from google.cloud import bigquery, storage
from datetime import datetime, timedelta

project_id = os.environ.get("project_id")
dataset = os.environ.get("dataset")
table_name = os.environ.get("table_name")
bucket_name = os.environ.get("bucket_name")
pasta_bucket = os.environ.get("pasta_bucket")
base_filename = os.environ.get("base_filename")

today = datetime.today()
day_str = today.strftime('%d')  
month_str = today.strftime('%m') 
year_str = today.strftime('%Y')  


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


def store_in_bigquery(filename):
    
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
    
    create_table_bigquery(df)


def upload_to_gcs(filename: str, data: pd.DataFrame):
    
    filename = filename
    
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{pasta_bucket}/{filename}')
    blob.upload_from_string(data.to_csv(index=False), content_type='text/csv')
    
    print(f"Arquivo '{filename}' enviado para o bucket '{bucket_name}' no diretório {pasta_bucket}.")
    store_in_bigquery(filename)


def requisicao_api():

    url = "https://www.ccee.org.br/login/pages/pld/index.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrando a div com a classe específica e o id
    div = soup.find('div', class_='tab-content', id='pills-tabContent')

    # Find all tab-pane divs within the main div
    tab_panes = div.find_all('div', class_='tab-pane')

    final_df = []

    # Iterate over each tab-pane and extract the table
    for tab_pane in tab_panes:
      # Encontrando a tabela e a data
      table = tab_pane.find('table', class_='table table-hover table-sm')
      date_element = tab_pane.find('small', class_='text-muted')
      date_str = date_element.text.split(':')[1].strip()  # Extraindo a data do texto

      # Convertendo a data para o formato datetime
      date_obj = datetime.strptime(date_str, '%d/%m/%Y')

      # Extraindo os dados da tabela
      headers = []
      data = []

      for th in table.find_all('th'):
          headers.append(th.text.strip())

      for tr in table.find_all('tr')[1:]:
          row = [td.text.strip() for td in tr.find_all('td')]
          row.insert(0, date_obj)  # Inserindo a data na primeira posição da linha
          data.append(row)

      # Criando o DataFrame e adicionando a coluna de data
      df = pd.DataFrame(data, columns=['Data'] + headers)
      final_df.append(df)

    # Concatenate all DataFrames into a single DataFrame
    final_df = pd.concat(final_df)
    
    # Derretendo o DataFrame (melt)
    df_melted = final_df.melt(id_vars=['Data', 'Hora'], 
                      value_vars=['Sudeste / Centro-Oeste', 'Sul', 'Nordeste', 'Norte'],
                      var_name='Região',
                      value_name='Valor')

    # Renomeando as colunas
    df_melted.columns = ['data', 'hora', 'região', 'valor']

    # Convertendo a coluna 'data' para o tipo datetime (se necessário)
    df_melted['data'] = pd.to_datetime(df_melted['data'])

    # Salvando o df   
    filename = f"{base_filename}{year_str}{month_str}{day_str}.csv"
    upload_to_gcs(filename, df_melted)


def main(request):
   requisicao_api()
   return "Dados carregados com sucesso!"