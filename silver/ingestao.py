# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import window

from delta.tables import *

import sys

sys.path.insert(0, '../lib')

from gcutils import db

# COMMAND ----------

# DBTITLE 1,Parâmetros
tb_origin = 'bronze_gc.tb_lobby_stats_player'
tb_target = 'silver_gc.tb_lobby_stats_player'

id_origin = 'idLobbyGame','idPlayer'
id_target = 'idLobbyGame','idPlayer'

strongly_date_origin = 'dtCreatedAt'
strongly_date_target = 'dtCreatedAt'

checkpoint_path = f'/mnt/datalake/silver/{tb_target.split(".")[-1]}_checkpoint'

# COMMAND ----------

# DBTITLE 1,Full load
query = db.import_query('queries/tb_lobby_stats_player.sql')

if not db.table_exists(*tb_target.split('.'), spark):
    query = query.replace(" Op,", "")
    print("Realizando a primeira carga...")
    df = db.etl(query, tb_origin, spark)
    df.coalesce(1).write.mode('overwrite').format('delta').saveAsTable(tb_target)
    print("ok.")

# COMMAND ----------

# DBTITLE 1,Stream
query = db.import_query('queries/tb_lobby_stats_player.sql')
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

stream.processAllAvailable()
stream.stop()
