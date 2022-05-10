with tb_player as (

select distinct idPlayer
from tb_lobby_stats_player

),

tb_random_players as (

    select idPlayer,
           abs(1.0 * random() % 10000)  as  randRank
    from tb_player

),

tb_selected_player as (

    select *
    from tb_random_players
    order by randRank desc, idPlayer desc
    limit {nPlayers}

),

tb_random_lobby as (

    select t1.*,
           t2.*,
           row_number() over (partition by t1.idPlayer order by random()) as rankLobby

    from tb_selected_player as t1
    left join tb_lobby_stats_player as t2
    on t1.idPlayer = t2.idPlayer

)

select 
    idLobbyGame + (select max(idLobbyGame) from tb_lobby_stats_player) as idLobbyGame,
    idPlayer,
    idRoom,
    qtKill,
    qtAssist,
    qtDeath,
    qtHs,
    qtBombeDefuse,
    qtBombePlant,
    qtTk,
    qtTkAssist,
    qt1Kill,
    qt2Kill,
    qt3Kill,
    qt4Kill,
    qt5Kill,
    qtPlusKill,
    qtFirstKill,
    vlDamage,
    qtHits,
    qtShots,
    qtLastAlive,
    qtClutchWon,
    qtRoundsPlayed,
    descMapName,
    vlLevel,
    qtSurvived,
    qtTrade,
    qtFlashAssist,
    qtHitHeadshot,
    qtHitChest,
    qtHitStomach,
    qtHitLeftAtm,
    qtHitRightArm,
    qtHitLeftLeg,
    qtHitRightLeg,
    flWinner,
    datetime((select max(dtCreatedAt) from tb_lobby_stats_player), '+1 day') as dtCreatedAt

from tb_random_lobby
where rankLobby = 1