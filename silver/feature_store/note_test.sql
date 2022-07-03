-- Databricks notebook source
select '{dtRef}' as dtRef,
       idPlayer,
       count(*) as qtMedals,
       count(distinct t1.idMedal) as qtDistMedals,
       sum(case when t2.descMedal in ('Membro Premium', 'Membro Plus') then 1 else 0 end) as qtAssinatura,
       max(case when t2.descMedal in ('Membro Premium', 'Membro Plus') and coalesce(dtRemove,dtExpiration) >= '{dtRef}' then 1 else 0 end) as flAssinante,
       sum(case when t2.descMedal in ('Tribo Gaules', 'Missão da Tribo') then 1 else 0 end) as qtTribo

from silver_gc.tb_players_medalha as t1

left join silver_gc.tb_medalha as t2
on t1.idMedal = t2.idMedal

where t1.dtCreatedAt < '{dtRef}'

group by 1, 2

-- COMMAND ----------

select * from silver_gc.tb_players_medalha

-- COMMAND ----------

Select distinct descMedal

from silver_gc.tb_players_medalha as t1

left join silver_gc.tb_medalha as t2
on t1.idMedal = t2.idMedal
