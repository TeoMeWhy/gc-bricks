select
      '{dtRef}' as dtRef,
      idPlayer,
      count(distinct date(dtCreatedAt)) as qtDays,
      count(idLobbyGame) as qtMatches,
      count(idLobbyGame) / count(distinct date(dtCreatedAt)) as avgMatchesPerDay,
      avg(vlDamage / qtRoundsPlayed) as avgADR,
      avg(qtDeath / qtRoundsPlayed) as avgDPR,
      avg(qtKill / qtRoundsPlayed) as avgKPR,
      avg(qtKill - qtDeath) as avgKDD,
      avg(qtKill) as avgQtKill,
      avg(qtAssist) as avgQtAssist,
      avg(qtDeath) as avgQtDeath,
      avg(qtHs) as avgQtHs,
      avg(propHsRate) as avgPropHsRate,
      avg(qtBombeDefuse) as avgQtBombeDefuse,
      avg(qtBombePlant) as avgQtBombePlant,
      avg(qtTk) as avgQtTk,
      avg(qtTkAssist) as avgQtTkAssist,
      avg(qt1Kill) as avgQt1Kill,
      avg(qt2Kill) as avgQt2Kill,
      avg(qt3Kill) as avgQt3Kill,
      avg(qt4Kill) as avgQt4Kill,
      avg(qt5Kill) as avgQt5Kill,
      avg(qtPlusKill) as avgQtPlusKill,
      avg(qtFirstKill) as avgQtFirstKill,
      avg(vlDamage) as avgVlDamage,
      avg(qtHits) as avgQtHits,
      avg(qtShots) as avgQtShots,
      avg(qtLastAlive) as avgQtLastAlive,
      avg(qtClutchWon) as avgQtClutchWon,
      avg(qtRoundsPlayed) as avgQtRoundsPlayed,
      avg(descMapName) as avgDescMapName,
      avg(case when descMapName = 'de_overpass' then 1 else 0 end) as pctMatchOverpass,
      avg(case when descMapName = 'de_vertigo' then 1 else 0 end) as pctMatchVertigo,
      avg(case when descMapName = 'de_nuke' then 1 else 0 end) as pctMatchNuke,
      avg(case when descMapName = 'de_train' then 1 else 0 end) as pctMatchTrain,
      avg(case when descMapName = 'de_mirage' then 1 else 0 end) as pctMatchMirage,
      avg(case when descMapName = 'de_dust2' then 1 else 0 end) as pctMatchDust2,
      avg(case when descMapName = 'de_inferno' then 1 else 0 end) as pctMatchInferno,
      avg(case when descMapName = 'de_overpass' and flWinner = 1 then 1 
               when descMapName = 'de_overpass' then 0 end) as pctWinRateOverpass,
      avg(case when descMapName = 'de_vertigo' and flWinner = 1 then 1 
               when descMapName = 'de_vertigo' then 0 end) as pctWinRateVertigo,
      avg(case when descMapName = 'de_nuke' and flWinner = 1 then 1 
               when descMapName = 'de_nuke' then 0 end) as pctWinRateNuke,
      avg(case when descMapName = 'de_train' and flWinner = 1 then 1 
               when descMapName = 'de_train' then 0 end) as pctWinRateTrain,
      avg(case when descMapName = 'de_mirage' and flWinner = 1 then 1 
               when descMapName = 'de_mirage' then 0 end) as pctWinRateMirage,
      avg(case when descMapName = 'de_dust2' and flWinner = 1 then 1 
               when descMapName = 'de_dust2' then 0 end) as pctWinRateDust2,
      avg(case when descMapName = 'de_inferno' and flWinner = 1 then 1 
               when descMapName = 'de_inferno' then 0 end) as pctWinRateInferno,
      avg(vlLevel) as avgVlLevel,
      avg(qtSurvived) as avgQtSurvived,
      avg(qtTrade) as avgQtTrade,
      avg(qtFlashAssist) as avgQtFlashAssist,
      avg(qtHitHeadshot) as avgQtHitHeadshot,
      avg(qtHitChest) as avgQtHitChest,
      avg(qtHitStomach) as avgQtHitStomach,
      avg(qtHitLeftAtm) as avgQtHitLeftAtm,
      avg(qtHitRightArm) as avgQtHitRightArm,
      avg(qtHitLeftLeg) as avgQtHitLeftLeg,
      avg(qtHitRightLeg) as avgQtHitRightLeg,
      avg(flWinner) as avgFlWinner

from silver_gc.tb_lobby_stats_player

where dtCreatedAt < date('{dtRef}')
and dtCreatedAt >= date('{dtRef}') - interval 30 days

group by 1, 2