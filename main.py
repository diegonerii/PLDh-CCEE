from utils.create_table_bigquery import create_table_bigquery
from utils.requisicao_api import requisicao_api
from utils.adding_quality_columns import adding_quality_columns
from utils.upload_to_gcs import upload_to_gcs
 
filename = requisicao_api[0]
df = requisicao_api()[1]

upload_to_gcs(filename, df)
df_bq = adding_quality_columns(filename)
create_table_bigquery(df_bq)

print("Dados carregados com sucesso!") 