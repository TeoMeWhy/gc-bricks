# Databricks notebook source
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql import window

from delta.tables import * 

import json
import time

def import_schema(table_name):
    with open(f'schemas/{table_name}.json', 'r') as open_file:
        schema = json.load(open_file)
    return types.StructType.fromJson(schema)

def table_exists(table_name):
    query = f'''show tables from bronze_gc like '{table_name}' '''
    df = spark.sql(query)
    return df.count() > 0
  

# COMMAND ----------

# DBTITLE 1,Setup do job
table_name = "tb_lobby_stats_player"

id_field = ['idLobbyGame', 'idPlayer']
strongly_date = 'dtCreatedAt'

full_load_path = f'/mnt/datalake/raw/gc/full-load/{table_name}'

cdc_path = f'/mnt/datalake/raw/gc/cdc/{table_name}'

table_schema = import_schema(table_name)

checkpoint_path = f'/mnt/datalake/bronze/gc/{table_name}_checkpoint'

stream_schema = table_schema[:]
stream_schema = stream_schema.add('Op', data_type=types.StringType(), nullable=False, metadata={})

# COMMAND ----------

# DBTITLE 1,Carga full-load
if not table_exists(table_name):
    print("Realizando a primeira carga...")
    df = spark.read.schema(table_schema).csv(full_load_path, header=True)
    df.write.format('delta').saveAsTable(f'bronze_gc.{table_name}')
    print("ok.")

# COMMAND ----------

# DBTITLE 1,Stream para CDC
def upsert_delta(df, batchId, delta_table, id_field, strongly_date):
    
    join = " and ".join([f'd.{i} = c.{i}' for i in id_field])
    
    w = window.Window.partitionBy(*id_field).orderBy(F.desc(strongly_date))
    cdc_data = (df.withColumn('rn', F.row_number().over(w))
                  .filter('rn=1')
                  .drop(F.col('rn')))
    
    (delta_table.alias("d")
    .merge(cdc_data.alias("c"), join) 
    .whenMatchedDelete(condition = "c.Op = 'D'")
    .whenMatchedUpdateAll(condition = "c.Op ='U'")
    .whenNotMatchedInsertAll(condition = "c.Op = 'I'")
    .execute())

    return None

delta_table = DeltaTable.forName(spark, f"bronze_gc.{table_name}")

df_stream = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'csv')
                  .option('header', 'true')
                  .schema(stream_schema)
                  .load(cdc_path))

stream = (df_stream.writeStream
                   .format('delta')
                   .foreachBatch(lambda df, batchId: upsert_delta(df, batchId, delta_table, id_field, strongly_date))
                   .option('checkpointLocation', checkpoint_path)
                   .start())

# COMMAND ----------

time.sleep(60)
stream.processAllAvailable()
stream.stop()
