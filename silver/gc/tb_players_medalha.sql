select Op,
       id,
       idPlayer,
       idMedal,
       case when dtCreatedAt > dtRemove then dtRemove else dtCreatedAt end as dtCreatedAt,
       dtExpiration,
       dtRemove,
       flActive

from {table}