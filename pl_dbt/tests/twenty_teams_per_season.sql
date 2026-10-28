select season, count(*) as n
from {{ ref('stg_pl_matches') }}
group by season
having count(*) != 380
