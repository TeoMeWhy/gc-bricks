# Databricks notebook source
import time

from pyspark.sql import functions as F
from pyspark.sql import window

from delta.tables import *

import sys

sys.path.insert(0, '../lib')

from gcutils import db

# COMMAND ----------

# DBTITLE 1,Parâmetros
tb_origin = dbutils.widgets.get('tb_origin')
tb_target = dbutils.widgets.get('tb_target')

id_origin = dbutils.widgets.get('id_origin').split(",")
id_target = dbutils.widgets.get('id_target').split(",")

strongly_date_origin = dbutils.widgets.get('strongly_date_origin')
strongly_date_target = dbutils.widgets.get('strongly_date_target')

checkpoint_path = f'/mnt/datalake/silver/gc/{tb_target.split(".")[-1]}_checkpoint'
table = tb_target.split(".")[-1]
database = "_".join(tb_target.split(".")[0].split("_")[1:])

# COMMAND ----------

# DBTITLE 1,Full load
query = db.import_query(f'{database}/{table}.sql')

if not db.table_exists(*tb_target.split('.'), spark):
    query_exec = query.replace(" Op,", "")
    print("Realizando a primeira carga...")
    df = db.etl(query_exec, tb_origin, spark)
    df.coalesce(1).write.mode('overwrite').format('delta').saveAsTable(tb_target)
    print("ok.")

# COMMAND ----------

# DBTITLE 1,Stream
delta_table = DeltaTable.forName(spark, tb_target)

def upsert_delta(df, batchId, query, delta_table, id_field, strongly_date):
    
    join = " and ".join([f'd.{i} = c.{i}' for i in id_field])
    
    w = window.Window.partitionBy(*id_field).orderBy(F.desc(strongly_date))
        
    df =(df.withColumn("Op", F.when(df._change_type == "insert","I")
                              .when(df._change_type == "update_preimage", "U")
                              .when(df._change_type == "update_postimage", "U")
                              .when(df._change_type == 'delete' ,"D"))
           .withColumn('rn', F.row_number().over(w))
           .filter('rn=1')
           .drop(F.col('rn')))
    
    view_name = f'{tb_target.split(".")[-1]}_view'
    df.createOrReplaceGlobalTempView(view_name)
    
    view_name = f'global_temp.{tb_target.split(".")[-1]}_view'
    cdc_data = db.etl(query, view_name, spark)
    
    (delta_table.alias("d")
                .merge(cdc_data.alias("c"), join) 
                .whenMatchedDelete(condition = "c.Op = 'D'")
                .whenMatchedUpdateAll(condition = "c.Op ='U'")
                .whenNotMatchedInsertAll(condition = "c.Op = 'I'")
                .execute())
    return None

df_stream = (spark.readStream
                  .format("delta")
                  .option("readChangeFeed", "true")
                  .option("startingVersion", 0)
                  .table(tb_origin))

stream = (df_stream.writeStream
                   .format('delta')
                   .foreachBatch(lambda df, batchId: upsert_delta(df, batchId, query, delta_table, id_target, strongly_date_target))
                   .option('checkpointLocation', checkpoint_path)
                   .start())

# COMMAND ----------

time.sleep(60)
stream.processAllAvailable()
stream.stop()
