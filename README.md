# PLDh

## Pacote Python para Extração e Armazenamento de Dados de PLD Horário da CCEE

Este repositório contém código Python para extrair dados da Companhia de Comercialização de Energia Elétrica (CCEE) e armazená-los no Google Cloud Storage (GCS) e BigQuery.
Funcionalidades

    Extrai dados de Preços de Liquidação de Diferenças (PLD) de várias regiões do site da CCEE.
    Converte os dados extraídos em um DataFrame do Pandas.
    Realiza o upload do DataFrame para o GCS como um arquivo CSV.
    Carrega o DataFrame do GCS para uma tabela do BigQuery.

## Instalação

### Pré-requisitos:

    Python 3.x
    Bibliotecas Python: requests, beautifulsoup4, pandas, google-cloud-bigquery, google-cloud-storage, pytz

### Instalando as bibliotecas:
Bash

pip install requests beautifulsoup4 pandas google-cloud-bigquery google-cloud-storage pytz

Use o código com cuidado.

### Variáveis de Ambiente:

O código utiliza variáveis de ambiente para armazenar informações de configuração. É necessário definir as seguintes variáveis antes de executar o script:

    project_id: ID do seu projeto do Google Cloud
    dataset: Nome do dataset no BigQuery
    table_name: Nome da tabela no BigQuery
    bucket_name: Nome do bucket no GCS
    pasta_bucket: Caminho da pasta dentro do bucket no GCS (opcional)
    base_filename: Base do nome do arquivo CSV a ser salvo (ex: "dados_pld_")

## Uso

    Defina as variáveis de ambiente conforme descrito acima.
    Execute o script main.py:

## Bash

python main.py

Use o código com cuidado.

## Como Contribuir

Estamos abertos a contribuições para melhorar este código. Sinta-se à vontade para enviar pull requests com correções, melhorias ou novas funcionalidades.