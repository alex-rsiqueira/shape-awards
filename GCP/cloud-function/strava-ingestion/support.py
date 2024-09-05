import os
import json 
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = 'raw'

df = pd.DataFrame()

def log_error(error, desc_e = None, project_id = PROJECT_ID, dataset_id = DATASET_ID, table_id = None):

    error_code = error.args[0]
    message = str(error)
    print(f"[ERROR] {desc_e} - {message}")

    log_table = pd.DataFrame(columns=['ingestion_dt', 'type', 'error_code', 'message', 'description', 'line_no', 'file_name', 'gateway', 'end_point', 'url', 'page'])
    ingestion_dt = datetime.now()
    new_line = [{'ingestion_dt': ingestion_dt, 'type': 'Error', 'error_code': error_code, 'message': message, 'description': desc_e, 'end_point':table_id }]
    log_table = pd.concat([log_table, pd.DataFrame(new_line, index=[0])], ignore_index=True)
    
     # Configurar o nome completo da tabela
    log_table_name = 'tb_nordigen_ingestion_log'

    insert_db(log_table,log_table_name,dataset_id,project_id)

##ADICIONAR TESTE DE ERRO DE BANCO

def identify_error(table_id,e,dataset_id,project_id):
    
    print('Registrando erro: ',e)
    
    if isinstance(e, json.JSONDecodeError):
        desc_e= 'Erro de decodificação JSON'
        log_error(e,desc_e,project_id,dataset_id,table_id)
    elif isinstance(e, requests.HTTPError):
        desc_e= 'Erro de requisição HTTP'
        log_error(e,desc_e,project_id,dataset_id,table_id)
    # elif isinstance(e, pyodbc.Error):
    #     desc_e= 'Erro de banco'
    #     # status_code = response.status_code
    #     log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    # elif isinstance(e, requests.RequestException):
        # pag=-1
        # desc_e= 'Erro de excessão da classe request'
        # # status_code = e.response.status_code if e.response is not None else 'Desconhecido'
        # log_error(project_id,dataset_id,table_id,e.args[0],str(e),desc_e)
    else:
        desc_e= 'Erro desconhecido'
        log_error(e,desc_e,project_id,dataset_id,table_id)

def insert_db(df,table_id,dataset_id,project_id):
     # Configurar o cliente do BigQuery
    try:
        # bq_client = bigquery.Client(project=project_id)

        # Configurar o nome completo da tabela
        table_ref = f'{project_id}.{dataset_id}.{table_id}'

        # Inserir o DataFrame na tabela (cria a tabela se não existir, trunca se existir)
        pd.io.gbq.to_gbq(df, destination_table=table_ref, if_exists='replace', project_id=project_id)

        print(f"Tabela populada com sucesso: {table_id}")
    except Exception as e:
        identify_error(table_id,e,dataset_id,project_id)

    # identify_error(table_id,e,dataset_id,project_id)

def refresh_token(client_id,client_secret,code,token,refresh_token):

    authorization = f'Bearer {token}'
    headers = {"Authorization": authorization}
    resp = requests.post(f'https://www.strava.com/oauth/token?client_id={client_id}&client_secret={client_secret}&code={code}&grant_type=refresh_token&refresh_token={refresh_token}', headers=headers)

    # Extract fields from response
    resp_dict = resp.json()
    new_token = resp_dict['access_token']
    new_expires_at = resp_dict['expires_at']
    new_expires_in = resp_dict['expires_in']

    # Save new values

    return "Token successfully refreshed."

def get_strava_accounts():
   
    bq_client = bigquery.Client(project=PROJECT_ID)
    query_job = bq_client.query(f"""SELECT UserID, Name, client_id, client_secret, authorization_code
                                    FROM `{PROJECT_ID}.trusted.tb_sheet_strava_account`
                                """)
    result = query_job.result()  # Waits for job to complete.
    account_list = [dict(row) for row in query_job]
   
    return account_list

def read_secret(secret_name):

    # Instantiate Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret path
    name = client.secret_version_path(PROJECT_ID, secret_name, 'latest')

    # Get secret content
    response = client.access_secret_version(request={"name": name})

    # Decode secret content
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value
