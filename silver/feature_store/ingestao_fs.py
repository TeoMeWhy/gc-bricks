# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

# DBTITLE 1,Imports
import datetime
import sys

from tqdm import tqdm

from databricks import feature_store

sys.path.insert(0, '../../lib')

from gcutils import db

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# DBTITLE 1,Funções
def data_range(dt_start, dt_stop):
    date = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    date_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
    
    dates = []
    while date <= date_stop:
        dates.append(date.strftime('%Y-%m-%d'))
        date += datetime.timedelta(days=1)
    
    return dates

def first_load(df, table_name, id_fields, partition_field, description=""):
    print("Criando a feature store...")
    fs.create_table(
        name = table_name,
        primary_keys = id_fields,
        df = df,
        partition_columns = partition_field,
        description = description)
    print("Ok.")
    return True
  
def incremental_load(df, table_name):
    print("Realizando carga incremental...")
    fs.write_table(
        name = table_name,
        df = df,
        mode = "merge")
    print("Ok.")
    
def backfill(dt_start, dt_stop, table_name, id_fields, partition_field, description=""):
    
    dates = data_range(dt_start, dt_stop)
    
    query_path = table_name.split(".")[-1] + '.sql'
    query = db.import_query(query_path)
        
    table_exists = db.table_exists(database_name, feature_store_name, spark)

    if not table_exists:
        query_exec = query.format(dtRef = dates.pop(0))
        df = spark.sql(query_exec)
        first_load(df, table_name, id_fields, partition_field, description="")
    
    for i in tqdm(dates):
        query_exec = query.format(dtRef = i)
        df = spark.sql(query_exec)
        incremental_load(df, table_name)

# COMMAND ----------

# DBTITLE 1,Setup
feature_store_name = "player_medal"
database_name = 'silver_gc_fs'

# id_fields = dbutils.widgets.get("id_fields").split(",")
# partition_field = dbutils.widgets.get("partition_field")

# dt_start = dbutils.widgets.get("dt_start")
# dt_stop = dbutils.widgets.get("dt_stop")

id_fields = "dtRef,idPlayer".split(",")
partition_field = "dtRef"

dt_start = '2021-10-15'
dt_stop = '2022-03-15'

table_name = f"{database_name}.{feature_store_name}"

# COMMAND ----------

# DBTITLE 1,Executando backfill
backfill(dt_start, dt_stop, table_name, id_fields, partition_field)
