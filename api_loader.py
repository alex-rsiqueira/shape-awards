# ===========================================================================================
# Data Criacao..: 21/08/2021
# Projeto.......: Api Loader
# Descricao.....: Classe genérica p consulta de apis com paralelismo e inserção no BQ
# Departamento..: Arquitetura e Engenharia de Dados
# Autor.........: Diogo Santiago
# Email.........: t_tivit.diogo.santiago@grupoboticario.com.br
# ===========================================================================================
import requests
import concurrent.futures as futures
from google.cloud import bigquery
from google.cloud import secretmanager
import pandas as pd
import io
import re
import json

class ApiLoader():

  config_required_keys = ["project_id", "raw_dataset_name", "raw_table_name"]
  client = bigquery.Client(project="data-architect-282615")
  do_save = True
  log_tablename = "auxiliar.tb_log_carga"

  def __init__(self, urls, config, mode="append", login_mode="user_pass", dedup_field=None, chunksize=300000, rename_mode="space"):
    
    if login_mode == "user_pass":
        self.config_required_keys += ["username", "password"]
    elif login_mode == "apikey":
        self.config_required_keys += ["api_key"]
    
    self._validate_config_keys(config)
    
    self.urls = urls
    self.config = config
    self.tablename = self.config["raw_dataset_name"] + '.' + self.config["raw_table_name"]
    self.mode = mode
    self.login_mode = login_mode
    self.dedup_field = dedup_field
    self.chunksize = chunksize
    self.rename_mode = rename_mode

  # Loads from BQ max value from a specific column
  @staticmethod
  def max_table_value(tablename, column, dtype="STRING"):
    sql = f"SELECT MAX(CAST({column} AS {dtype})) AS max_value FROM {tablename}"
    query_job = ApiLoader.client.query(sql)
    result = query_job.result()  # Waits for job to complete.
    max_value = next(result).max_value
    return max_value
    
  # Loads credentials from Secret Manager
  @staticmethod
  def load_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return json.loads(payload)    
	
  def load_url(self, url, timeout=5):
    auth = None
    
    if self.login_mode == "user_pass":
        auth = requests.auth.HTTPBasicAuth(self.config['username'], self.config['password'])
		
    res = requests.get(url, auth=auth)
    return url, res.content.decode("utf-8")

  # Load urls with paralellism
  def load_urls(self, timeout=5):
    urls = [self.urls] if type(self.urls) is str else self.urls
    items = []
	
    max_workers = max(1, len(urls) // 2)
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
      future_urls = (executor.submit(self.load_url, url, timeout) for url in urls)
	  
      for future in futures.as_completed(future_urls):
        items.append(future.result())
		
    return items
    # Checks for empty responses, if found, interrupts...
    #if empty_items := list(filter(lambda item: not item[1], items)):
    # empty_urls = list(dict(empty_items).keys())
    # raise Exception(f"[ERROR] Could not reach the following urls: {empty_urls}")
    
    df = pd.concat([pd.read_csv(io.StringIO(item[1])) for item in items])
    return df.reset_index(drop=True)
  
  # Uses the appropriate method to convert column names
  def rename_columns(self, columns):
    function = getattr(self, self.rename_mode + "_to_snake")
    # apply case
    columns = [function(c) for c in columns] 
    # prefix with underline columns with name forbidden by BQ(e.g. starts with numbers, special chars, etc)
    columns = [f"_{c}" if re.match("[^a-z_].+", c) else c for c in columns] 
    return columns

  # Convert column names from scace/underline to snakecase
  def space_to_snake(self, string):
    groups = re.findall("(\w+)", string)
    return '_'.join(groups).lower()

  # Convert column names from camelcase to snakecase
  def camel_to_snake(self, string):
    string = string[0].upper() + string[1:]
    groups = re.findall("([A-z\d][a-z\d]+)", string)
    return '_'.join(groups).lower()

  # Change columns with custom transformations, implement direct in instance object
  def transformations(self, df, **kwargs):
    return df

  # Truncates table
  def truncate_table(self, table):
    print(f"[INFO] Truncanting table {table!r}")
    sql = f"TRUNCATE {table}"
    self.client.query(sql).result()

  # Deduplicate table based on fields
  def deduplicate_table(self, table, dedup_filter):
    print(f"[INFO] Deduping table {table!r}")
    sql = f"DELETE FROM {table} WHERE {dedup_filter}"
    self.client.query(sql).result()

  # Saves data to BigQuery
  def save_bq(self, df):
    
    if self.mode == "full" and self.do_save:
      self.truncate_table(self.tablename)

    if self.mode == "append" and self.dedup_field and self.do_save:
      values_to_delete = str(df[self.dedup_field].unique().tolist())[1:-1]
      dedup_filter = f"{self.dedup_field} IN ({values_to_delete})"
      self.deduplicate_table(self.tablename, dedup_filter)
    
    if self.do_save:
      print("[INFO] Saving to BQ...")
      df.to_gbq(self.tablename, if_exists="append", chunksize=self.chunksize, project_id=self.config['project_id'])
    
    print(f"[INFO] Inserted {len(df)} rows in table {self.tablename!r}.")

  # Runs the pipeline, consuming data from an arbitrary API and sending to BigQuery  
  def run(self):
    df = self.load_urls(self.urls)
    df.columns = self.rename_columns(df.columns)
    
    # Convertion to string fields
    df = df.fillna(value='').astype(str)
    df = self.transformations(df)
    df = df.astype(str) # double check to guarantee all string after custom transform
    self.save_bq(df)
    return df

  # Saves log from execution
  def save_log(self, log):
    print("[INFO] Saving logs ...")
    log = {k.upper():[v] for k,v in log.items()}
    df = pd.DataFrame(log)
    df.to_gbq(self.log_tablename, if_exists="append", project_id=self.config['project_id'])
    return df

  # Validates all necessary keys for execution
  def _validate_config_keys(self, config):
    missing_keys = self.config_required_keys - config.keys()
    
    if missing_keys:
      raise Exception(f"[ERROR] Config dict have missing values: {list(missing_keys)}")
