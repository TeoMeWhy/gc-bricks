# Databricks notebook source
import datetime
import sys

from tqdm import tqdm

from databricks import feature_store

sys.path.insert(0, '../../lib')

from gcutils import db

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

df_target = spark.sql('''
with tb_subs as (

select 
    t1.id,    
    t1.idPlayer,
    t1.dtCreatedAt

from silver_gc.tb_players_medalha as t1
    
left join silver_gc.tb_medalha as t2
on t1.idMedal = t2.idMedal

where t2.descMedal in ('Membro Premium', 'Membro Plus')

)

select 
       t1.idPlayer,
       t1.dtRef,
       case when t2.id is not null then 1 else 0 end as flSub

from silver_gc_fs.player_gameplay as t1

left join tb_subs as t2
on t1.idPlayer = t2.idPlayer
and date(t1.dtRef) >= date_sub(t2.dtCreatedAt,15)
and date(t1.dtRef) < t2.dtCreatedAt


''')

# COMMAND ----------

fs_gameplay = fs.get_table("silver_gc_fs.player_gameplay")
features = list(set(fs_gameplay.features) - set(fs_gameplay.primary_keys))

feature_lookups = [
    feature_store.FeatureLookup(
      table_name = fs_gameplay.name,
      feature_names = features,
      lookup_key = fs_gameplay.primary_keys,
    ),
  ]

# COMMAND ----------

training_set = fs.create_training_set(
    df = df_target,
    feature_lookups = feature_lookups,
    label = 'flSub',
    exclude_columns = ['idPlayer','dtRef']
)

# COMMAND ----------

training_df = training_set.load_df()
