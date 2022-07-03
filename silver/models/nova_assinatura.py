# Databricks notebook source
# DBTITLE 1,Setup
from databricks import feature_store

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# DBTITLE 1,Lookups
fs_player_gameplay = fs.get_table('silver_gc_fs.player_gameplay')
features_player_gameplay = list(set(fs_player_gameplay.features) - set(fs_player_gameplay.primary_keys))
features_player_gameplay

fs_player_medal = fs.get_table('silver_gc_fs.player_medal')
features_player_medal = list(set(fs_player_medal.features) - set(fs_player_medal.primary_keys))
features_player_medal

feature_lookups = [
    
    feature_store.FeatureLookup(
      table_name = 'silver_gc_fs.player_gameplay',
      feature_names = features_player_gameplay,
      lookup_key = fs_player_gameplay.primary_keys
    ),
    
    feature_store.FeatureLookup(
      table_name = 'silver_gc_fs.player_medal',
      feature_names = features_player_medal,
      lookup_key = fs_player_medal.primary_keys
    )
]

# COMMAND ----------

# DBTITLE 1,Variável resposta

df_training = spark.sql('''
with tb_assinatura as (

    select *,
           1 as flSub

    from silver_gc.tb_players_medalha as t1

    left join silver_gc.tb_medalha as t2
    on t1.idMedal = t2.idMedal

    where t2.descMedal in ('Membro Premium', 'Membro Plus')
)

select distinct t1.dtRef,
       t1.idPlayer,
       coalesce(t2.flSub, 0) as flagSub

from silver_gc_fs.player_gameplay as t1

left join tb_assinatura as t2
on t1.idPlayer = t2.idPlayer
and t1.dtRef <= t2.dtCreatedAt
and t1.dtRef > t2.dtCreatedAt - interval 10 days
''')

# COMMAND ----------

# DBTITLE 1,Definição da base de treino
df_training = fs.create_training_set(
  df=df_training,
  feature_lookups = feature_lookups,
  label = 'flagSub',
  exclude_columns = ['dtRef', 'idPlayer']
)

training_df = df_training.load_df()
