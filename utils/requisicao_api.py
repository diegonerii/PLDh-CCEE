import os
import requests
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime

base_filename = os.environ.get("base_filename")

today = datetime.today()
day_str = today.strftime('%d')  
month_str = today.strftime('%m') 
year_str = today.strftime('%Y')  

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
    
    return (filename, df_melted)